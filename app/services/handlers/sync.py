"""Sync task handler implementation."""

import logging
import shlex
from typing import Any, Dict, List, Optional

import yaml
from jinja2 import Template
from kubernetes.client import V1Pod

from ..constants import (
    K8S_SYNC_JOB_LABEL,
    K8S_SYNC_JOB_NAME_PREFIX,
    K8S_VERIFIER_JOB_LABEL,
    K8S_VERIFIER_JOB_NAME_PREFIX,
    MOUNT_VERIFY_CMD,
    SYNC_JOB_TEMPLATE,
    SYNC_VERIFIER_TEMPLATE,
)
from ..directory import make_volume_name_from_path, match_allowed_directory
from ..errors import (
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskJobError,
    TaskTemplateRenderError,
)
from ..kube import PodCheckResult, VolcanoJobRunner
from ...models.schemas import TaskRequest, TaskResult
from .base import BaseTaskHandler

logger = logging.getLogger(__name__)


class SyncTaskHandler(BaseTaskHandler):
    def __init__(self, job_runner: VolcanoJobRunner, queue_name: str = "high-q"):
        self.job_runner = job_runner
        self.queue_name = queue_name

    async def validate(self, request: TaskRequest) -> None:
        if not isinstance(request.parameters, dict):
            raise TaskInvalidParametersError(
                request.task_id,
                request.service,
                [
                    "parameters must be a dictionary for sync service",
                ],
            )
        await self._check_format_sync(request)

    async def execute(self, request: TaskRequest) -> TaskResult:
        params = request.parameters or {}
        task_id = request.task_id

        src: str = params.get("src")
        dst: str = params.get("dst")
        options: str = params.get("options", "")

        src_mount_path, src_info = match_allowed_directory(src)
        if src_info is None:
            raise TaskInvalidDirectoryError(task_id, request.service, src)

        dst_mount_path, dst_info = match_allowed_directory(dst)
        if dst_info is None:
            raise TaskInvalidDirectoryError(task_id, request.service, dst)

        verifier_obj = self._render_template(
            SYNC_VERIFIER_TEMPLATE,
            {
                "task_id": task_id,
                "verifier_label": K8S_VERIFIER_JOB_LABEL,
                "queue_name": self.queue_name,
                "verifier_image": "rts2411:5000/ubuntu:24.04",
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
        await self.job_runner.create_job(verifier_obj)

        try:
            verifier_pods = await self.job_runner.wait_for_pods_ready(
                label_selector=f"{K8S_VERIFIER_JOB_LABEL}={task_id}",
                expected=2,
                timeout=180,
            )
            check_outputs = await self._verify_mounts(
                verifier_pods, src_mount_path, dst_mount_path
            )
        finally:
            await self.job_runner.delete_job(verifier_job_name)

        sync_obj = self._render_template(
            SYNC_JOB_TEMPLATE,
            {
                "task_id": task_id,
                "queue_name": self.queue_name,
                "job_label": K8S_SYNC_JOB_LABEL,
                "src_path": src,
                "dst_path": dst,
                "dsync_options": options,
                "src_volume_name": make_volume_name_from_path(src_mount_path),
                "src_mount_path": src_mount_path,
                "dst_volume_name": make_volume_name_from_path(dst_mount_path),
                "dst_mount_path": dst_mount_path,
            },
        )

        sync_job_name = f"{K8S_SYNC_JOB_NAME_PREFIX}-{task_id}"
        await self.job_runner.create_job(sync_obj)

        try:
            pods = await self.job_runner.wait_for_completion(
                label_selector=f"{K8S_SYNC_JOB_LABEL}={task_id}", timeout=1800
            )
            logs = await self._collect_logs(pods)
        finally:
            await self.job_runner.delete_job(sync_job_name)

        return TaskResult(
            pod_status="Succeeded",
            launcher_output=self._format_output(check_outputs, logs),
        )

    async def _verify_mounts(
        self, pods: list[V1Pod], src_mount_path: str, dst_mount_path: str
    ) -> list[PodCheckResult]:
        checks: list[PodCheckResult] = []

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
            logger.info("[Task] mount check on %s => %s", pod_name, output)

            if "__OK__" not in output:
                raise TaskJobError(pod_name, f"Mount verification failed: {output}")

            checks.append(PodCheckResult(name=pod_name, output=output))

        return checks

    async def _collect_logs(self, pods: list[V1Pod]) -> Dict[str, str]:
        logs: Dict[str, str] = {}
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

    def _format_output(
        self, checks: List[PodCheckResult], logs: Dict[str, str]
    ) -> str:
        lines = ["[sync] mount verification:"]
        for check in checks:
            lines.append(f"- {check.name}: {check.output}")
        lines.append("[sync] pod logs:")
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
                raise TaskInvalidDirectoryError(request.task_id, request.service, src)

        if dst is not None:
            _, dst_info = match_allowed_directory(dst)
            if dst_info is None:
                raise TaskInvalidDirectoryError(request.task_id, request.service, dst)

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


__all__ = ["SyncTaskHandler"]
