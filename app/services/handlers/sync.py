"""Sync task handler implementation."""
import logging
import shlex
import pwd
from typing import Any, Dict, Optional, Tuple

import yaml
from jinja2 import Template
from kubernetes.client import V1Pod

from ..constants import (
    ALLOWED_DIRECTORIES,
    K8S_SYNC_JOB_LABEL,
    K8S_SYNC_JOB_NAME_PREFIX,
    K8S_VERIFIER_JOB_LABEL,
    K8S_VERIFIER_JOB_NAME_PREFIX,
    K8S_VERIFIER_JOB_IMAGE,
    K8S_VOLCANO_HIGH_PRIO_Q,
    K8S_VOLCANO_LOW_PRIO_Q,
    MOUNT_VERIFY_CMD,
    PATHTYPE_VERIFY_CMD,
    OWNERSHIP_VERIFY_SRC_FILE_CMD,
    OWNERSHIP_VERIFY_SRC_DIR_CMD,
    OWNERSHIP_VERIFY_DST_CMD,
    K8S_SYNC_JOB_TEMPLATE,
    K8S_SYNC_VERIFIER_TEMPLATE,
)
from ..directory import make_volume_name_from_path, match_allowed_directory
from ..errors import (
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskJobError,
    TaskTemplateRenderError,
)
from ..kube import PodMountCheckResult, PodPathCheckResult, VolcanoJobRunner
from ..state_store import StateStore
from ...models.schemas import CancelRequest, TaskRecord, TaskRequest, TaskResult
from .base import BaseTaskHandler

logger = logging.getLogger(__name__)


class SyncTaskHandler(BaseTaskHandler):
    def __init__(self, job_runner: VolcanoJobRunner, state_store: StateStore):
        self.job_runner = job_runner
        self.state_store = state_store

    async def validate(self, request: TaskRequest) -> None:
        if not isinstance(request.parameters, dict):
            raise TaskInvalidParametersError(
                request.task_id,
                request.service,
                ["parameters must be a dictionary for sync service"],
            )
        await self._check_format_sync(request)

    async def execute(self, request: TaskRequest) -> TaskResult:
        params = request.parameters or {}
        task_id = request.task_id
        user_id = request.user_id
        pwd.getpwnam(user_id)
        src: str = params.get("src")
        dst: str = params.get("dst")
        pod_status: dict[str, str] = {}
        logs: dict[str, str] = {}

        src_mount_path, src_info = match_allowed_directory(src)
        if src_info is None:
            raise TaskInvalidDirectoryError(
                task_id, src, f"Invalid path to service '{request.service}'"
            )

        dst_mount_path, dst_info = match_allowed_directory(dst)
        if dst_info is None:
            raise TaskInvalidDirectoryError(
                task_id, dst, f"Invalid path to service '{request.service}'"
            )

        verifier_obj = self._render_template(
            K8S_SYNC_VERIFIER_TEMPLATE,
            {
                "task_id": task_id,
                "verifier_label": K8S_VERIFIER_JOB_LABEL,
                "queue_name": K8S_VOLCANO_HIGH_PRIO_Q,
                "verifier_image": K8S_VERIFIER_JOB_IMAGE,
                "src_path": src,
                "dst_path": dst,
                "src_checker_node": {src_info["label"]: "true"},
                "dst_checker_node": {dst_info["label"]: "true"},
                "src_volume_name": make_volume_name_from_path(src_mount_path),
                "src_mount_path": src_mount_path,
                "dst_volume_name": make_volume_name_from_path(dst_mount_path),
                "dst_mount_path": dst_mount_path,
            },
        )

        verifier_job_name = f"{K8S_VERIFIER_JOB_NAME_PREFIX}-{task_id}"
        await self._add_active_job(task_id, verifier_job_name, "Registered verifier job")

        try:
            await self.job_runner.create_job(verifier_obj)
            verifier_pods = await self.job_runner.wait_for_pods_ready(
                label_selector=f"{K8S_VERIFIER_JOB_LABEL}={task_id}",
                expected=2,
                timeout=180,
            )

            await self._verify_mount(task_id, verifier_pods, src_mount_path, dst_mount_path)

            src_path_type, dst_path_type = await self._verify_pathtype(
                task_id, verifier_pods, src, dst
            )

            await self._verify_ownership(
                task_id,
                user_id,
                verifier_pods,
                src,
                src_path_type,
                dst,
                dst_path_type,
            )
            
            TEST_CMD = "while true; do date '+%Y-%m-%d %H:%M:%S'; sleep 1; done"
            logger.info(f"Run infinite loop on {verifier_pods[0].metadata.name}")
            await self.job_runner.exec_in_pod(verifier_pods[0].metadata.name,
                    ["/bin/bash", "-c", TEST_CMD])
        finally:
            try:
                await self.job_runner.delete_job(verifier_job_name)
            except TaskJobError as exc:
                logger.warning(
                    "[Task %s] Failed to delete verifier job %s: %s",
                    task_id,
                    verifier_job_name,
                    exc,
                )
            finally:
                await self._remove_active_job(task_id, verifier_job_name, "Verifier job cleaned up")

        return TaskResult(
            pod_status="Succeeded",
            launcher_output=self._format_output(pod_status, logs),
        )

    async def cancel(self, request: CancelRequest, state: TaskRecord | None = None) -> None:
        task_state = state
        if task_state is None:
            task_state = await self.state_store.get_task(request.task_id)

        if task_state is None:
            return

        jobs = list(task_state.active_jobs)
        if not jobs:
            await self.state_store.append_log(request.task_id, "No active jobs to cancel")
            return

        for job_name in jobs:
            try:
                await self.job_runner.delete_job(job_name)
                await self.state_store.append_log(
                    request.task_id, f"Cancellation sent to job {job_name}"
                )
            except TaskJobError as exc:
                logger.warning("[Task %s] Failed to cancel job %s: %s", request.task_id, job_name, exc)

        await self.state_store.clear_active_jobs(
            request.task_id, "Cleared job references after cancellation"
        )

    async def _verify_mount(
        self, task_id: str, pods: list[V1Pod], src_mount_path: str, dst_mount_path: str
    ) -> None:
        checks: list[PodMountCheckResult] = []

        for pod in pods:
            pod_name = pod.metadata.name
            if "src-checker" in pod_name:
                mount_point = src_mount_path
            elif "dst-checker" in pod_name:
                mount_point = dst_mount_path
            else:
                raise TaskJobError(pod_name, "Verifier pod naming is invalid")

            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    ["/bin/bash", "-c", MOUNT_VERIFY_CMD.format(mount_point=mount_point)],
                )
            ).strip()
            logger.info("[Task %s] 'mount' check of %s => %s", task_id, mount_point, output)
            checks.append(PodMountCheckResult(name=pod_name, output=output))

        is_mount_verified = all(item.output == "__TRUE__" for item in checks)
        if not is_mount_verified:
            raise TaskJobError(task_id, f"Invalid mount point: {checks}")

    async def _verify_pathtype(
        self, task_id: str, pods: list[V1Pod], src_path: str, dst_path: str
    ) -> Tuple[Optional[str], Optional[str]]:
        checks: list[PodPathCheckResult] = []

        for pod in pods:
            pod_name = pod.metadata.name
            if "src-checker" in pod_name:
                target_path = src_path
                type_path = "src"
            elif "dst-checker" in pod_name:
                target_path = dst_path
                type_path = "dst"
            else:
                raise TaskJobError(pod_name, "Verifier pod naming is invalid")

            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    ["/bin/bash", "-c", PATHTYPE_VERIFY_CMD.format(target_path=target_path)],
                )
            ).strip()
            if type_path == "src":
                logger.info("[Task %s] src type => %s", task_id, output)

            if type_path == "dst":
                logger.info("[Task %s] dst type => %s", task_id, output)
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        src_path_type = next((r.output for r in checks if "src-checker" in r.name), None)
        dst_path_type = next((r.output for r in checks if "dst-checker" in r.name), None)
        return src_path_type, dst_path_type

    async def _verify_ownership(
        self,
        task_id: str,
        user_id: str,
        pods: list[V1Pod],
        src_path: str,
        src_path_type: str,
        dst_path: str,
        dst_path_type: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        checks: list[PodPathCheckResult] = []

        if src_path_type == "__NOT_FOUND__":
            raise TaskInvalidDirectoryError(task_id, src_path, "Cannot find the src path")
        if src_path_type == "__FILE__":
            ownership_verify_src_cmd = OWNERSHIP_VERIFY_SRC_FILE_CMD
        elif src_path_type == "__DIR__":
            ownership_verify_src_cmd = OWNERSHIP_VERIFY_SRC_DIR_CMD
            if not src_path.startswith("/") or src_path.count("/") == 1 or src_path in ALLOWED_DIRECTORIES:
                raise TaskInvalidDirectoryError(task_id, src_path, "Not allowed path")
        else:
            raise TaskInvalidDirectoryError(
                task_id, src_path, f"Unknown src path type: {src_path_type}"
            )

        if dst_path_type == "__DIR__":
            if not dst_path.startswith("/") or dst_path.count("/") == 1 or dst_path in ALLOWED_DIRECTORIES:
                raise TaskInvalidDirectoryError(task_id, dst_path, "Not allowed path")
        else:
            raise TaskInvalidDirectoryError(task_id, dst_path, f"Dst path is not a directory - {dst_path_type}")

        for pod in pods:
            pod_name = pod.metadata.name
            if "src-checker" in pod_name:
                target_path = src_path
                target_cmd = ownership_verify_src_cmd
                type_path = "src"
            elif "dst-checker" in pod_name:
                target_path = dst_path
                target_cmd = OWNERSHIP_VERIFY_DST_CMD
                type_path = "dst"
            else:
                raise TaskJobError(pod_name, "Verifier pod naming is invalid")

            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    ["/bin/bash", "-c", target_cmd.format(user_id=user_id, target_path=target_path)],
                )
            ).strip()
            logger.info("[Task %s] %s ownership => %s", task_id, type_path, output)
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        src_ownership = next((r.output for r in checks if "src-checker" in r.name), None)
        if src_ownership != "__TRUE__":
            raise TaskInvalidDirectoryError(
                task_id, src_path, f"Src path ownership check: {src_ownership}"
            )

        dst_ownership = next((r.output for r in checks if "dst-checker" in r.name), None)
        if dst_ownership != "__TRUE__":
            raise TaskInvalidDirectoryError(
                task_id, dst_path, f"Dst path ownership check: {dst_ownership}"
            )

        return src_ownership, dst_ownership

    async def _collect_logs(self, pods: list[V1Pod]) -> dict[str, str]:
        logs: dict[str, str] = {}
        for pod in pods:
            pod_name = pod.metadata.name
            try:
                logs[pod_name] = await self.job_runner.get_pod_logs(pod_name)
            except TaskJobError as exc:
                logger.warning("Failed to read logs from %s: %s", pod_name, exc)
        return logs

    def _render_template(self, template_path: str, context: Dict[str, Any]) -> dict:
        try:
            with open(template_path) as f:
                template = Template(f.read())
            rendered = template.render(**context)
            return yaml.safe_load(rendered)
        except Exception as exc:  # pragma: no cover - render failure path
            raise TaskTemplateRenderError(template_path, exc) from exc

    def _format_output(self, pod_status: dict[str, str], logs: dict[str, str]) -> str:
        lines = ["Mount verification:"]
        for pod, status in pod_status.items():
            lines.append(f"- {pod} status: {status}")
        lines.append("Pod logs:")
        for pod, content in logs.items():
            lines.append(f"- {pod} logs:\n{content}")
        return "\n".join(lines)

    async def _check_format_sync(self, request: TaskRequest) -> None:
        params: Dict[str, Any] = request.parameters or {}
        errors: list[str] = []

        src = params.get("src")
        dst = params.get("dst")

        if not isinstance(src, str) or not src:
            errors.append(f"'src' must be a non-empty string, got {repr(src)}")

        if not isinstance(dst, str) or not dst:
            errors.append(f"'dst' must be a non-empty string, got {repr(dst)}")

        if isinstance(src, str) and not src.startswith("/"):
            errors.append(f"'src' must be an absolute path starting with '/', got {src!r}")
        if isinstance(dst, str) and not dst.startswith("/"):
            errors.append(f"'dst' must be an absolute path starting with '/', got {dst!r}")

        for key, path in (("src", src), ("dst", dst)):
            if isinstance(path, str) and ".." in path.split("/"):
                errors.append(f"'{key}' must not contain '..' segments, got {path!r}")

        options = params.get("options")
        if options is not None:
            if not isinstance(options, str):
                errors.append(
                    f"'options' must be a string if provided, got {type(options).__name__}"
                )
            else:
                errors.extend(self._validate_dsync_options(options))

        if errors:
            raise TaskInvalidParametersError(request.task_id, request.service, errors)

        if src is not None:
            _, src_info = match_allowed_directory(src)
            if src_info is None:
                raise TaskInvalidDirectoryError(
                    request.task_id, src, f"Invalid path to service '{request.service}'"
                )

        if dst is not None:
            _, dst_info = match_allowed_directory(dst)
            if dst_info is None:
                raise TaskInvalidDirectoryError(
                    request.task_id, dst, f"Invalid path to service '{request.service}'"
                )

    def _validate_dsync_options(self, options: str) -> list[str]:
        errors: list[str] = []

        allowed_flags = {
            "--batch-files",
            "--bufsize",
            "--chunksize",
            "--xattrs",
            "--contents",
            "--no-dereference",
            "--direct",
            "--open-noatime",
            "--delete",
        }

        flags_with_value: Dict[str, Any] = {
            "--batch-files": "int",
            "--bufsize": "str",
            "--chunksize": "str",
            "--xattrs": {"none", "all", "non-lustre", "libattr"},
        }

        try:
            tokens = shlex.split(options)
        except ValueError as e:
            errors.append(f"Failed to parse dsync options: {e}")
            return errors

        i = 0
        while i < len(tokens):
            flag = tokens[i]

            if flag not in allowed_flags:
                errors.append(f"Unsupported dsync option: {flag!r}")
                i += 1
                continue

            if flag in flags_with_value:
                if i + 1 >= len(tokens):
                    errors.append(f"Option {flag!r} requires a value")
                    i += 1
                    continue

                value = tokens[i + 1]
                expected = flags_with_value[flag]

                if expected == "int":
                    try:
                        v = int(value)
                        if v <= 0:
                            errors.append(f"Option {flag!r} must be > 0, got {v}")
                    except ValueError:
                        errors.append(f"Option {flag!r} must be an integer, got {value!r}")
                elif isinstance(expected, set):
                    if value not in expected:
                        errors.append(
                            f"Option {flag!r} must be one of {sorted(expected)}, got {value!r}"
                        )
                elif expected == "str" and not value.strip():
                    errors.append(f"Option {flag!r} must be a non-empty string")

                i += 2
            else:
                i += 1

        return errors

    async def _add_active_job(self, task_id: str, job_name: str, message: str) -> None:
        updated = await self.state_store.add_active_job(task_id, job_name, message)
        if updated:
            logger.info("[Task %s] added active job %s", task_id, job_name)

    async def _remove_active_job(self, task_id: str, job_name: str, message: str) -> None:
        updated = await self.state_store.remove_active_job(task_id, job_name, message)
        if updated:
            logger.info("[Task %s] removed active job %s", task_id, job_name)


__all__ = ["SyncTaskHandler"]
