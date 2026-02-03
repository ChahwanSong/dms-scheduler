"""RM task handler implementation."""

import asyncio
import logging
import pwd
import shlex
from contextlib import suppress
from typing import Any, Dict, Optional

import yaml
from jinja2 import Template
from kubernetes.client import V1Pod

from ..cmds import MOUNT_VERIFY_CMD, PATHTYPE_VERIFY_CMD, RM_OWNERSHIP_VERIFY_CMD
from ..constants import (
    K8S_RM_JOB_IMAGE,
    K8S_RM_JOB_LABEL,
    K8S_RM_JOB_NAME_PREFIX,
    K8S_RM_JOB_TEMPLATE,
    K8S_RM_VERIFIER_JOB_IMAGE,
    K8S_RM_VERIFIER_JOB_LABEL,
    K8S_RM_VERIFIER_JOB_NAME_PREFIX,
    K8S_RM_VERIFIER_TEMPLATE,
    K8S_VOLCANO_HIGH_PRIO_Q,
    K8S_VOLCANO_LOW_PRIO_Q,
)
from ..directory import make_volume_name_from_path, match_allowed_directory
from ..errors import (
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskJobError,
    TaskNotFoundError,
    TaskTemplateRenderError,
)
from ..kube import ExecResult, PodMountCheckResult, PodPathCheckResult, VolcanoJobRunner
from ..state_store import StateStore
from ...models.schemas import CancelRequest, TaskRecord, TaskRequest, TaskResult, TaskStatus
from .base import BaseTaskHandler

logger = logging.getLogger(__name__)


class RmTaskHandler(BaseTaskHandler):
    def __init__(self, job_runner: VolcanoJobRunner, state_store: StateStore):
        self.job_runner = job_runner
        self.state_store = state_store

    async def validate(self, request: TaskRequest) -> None:
        if not isinstance(request.parameters, dict):
            raise TaskInvalidParametersError(
                request.task_id,
                request.service,
                ["parameters must be a dictionary for rm service"],
            )
        await self._check_format_rm(request)

    async def execute(self, request: TaskRequest) -> TaskResult:
        task_id = request.task_id
        user_id = request.user_id
        pwd.getpwnam(user_id)

        params = request.parameters or {}
        target_path: str = params.get("path")
        options = params.get("options") or ""

        mount_path, mount_info = match_allowed_directory(target_path)
        if mount_info is None:
            raise TaskInvalidDirectoryError(
                task_id, target_path, f"Invalid path to service '{request.service}'"
            )

        await self._ensure_task_running(task_id)
        verifier_obj = self._render_template(
            K8S_RM_VERIFIER_TEMPLATE,
            {
                "task_id": task_id,
                "job_name_prefix": K8S_RM_VERIFIER_JOB_NAME_PREFIX,
                "verifier_job_label": K8S_RM_VERIFIER_JOB_LABEL,
                "queue_name": K8S_VOLCANO_HIGH_PRIO_Q,
                "verifier_image": K8S_RM_VERIFIER_JOB_IMAGE,
                "target_path": target_path,
                "target_checker_node": {mount_info["label"]: "true"},
                "target_volume_name": make_volume_name_from_path(mount_path),
                "target_mount_path": mount_path,
            },
        )

        verifier_job_name = f"{K8S_RM_VERIFIER_JOB_NAME_PREFIX}-{task_id}"
        await self._add_active_job(task_id, verifier_job_name, "Registered rm verifier job")

        try:
            await self._ensure_task_running(task_id)
            await self.job_runner.create_job(verifier_obj)

            await self._ensure_task_running(task_id)
            verifier_pods = await self.job_runner.wait_for_pods_ready(
                label_selector=f"{K8S_RM_VERIFIER_JOB_LABEL}={task_id}",
                expected=1,
                timeout=180,
            )

            await self._verify_mount(task_id, verifier_pods, mount_path)
            path_type = await self._verify_pathtype(task_id, verifier_pods, target_path)

            if user_id != "root":
                await self._verify_ownership(task_id, user_id, verifier_pods, target_path)

            if path_type == "__DIR__" and not self._has_recursive_option(options):
                raise TaskInvalidParametersError(
                    task_id,
                    request.service,
                    ["rm requires a recursive option (-r/--recursive) when target is a directory"],
                )
        finally:
            try:
                await self._cleanup_job(
                    task_id, verifier_job_name, "Rm verifier job cleaned up"
                )
            except TaskJobError:
                logger.warning(
                    f"[Task {task_id}] Failed to clean up verifier job {verifier_job_name}"
                )

        queue_name = await self._get_task_queue_name(task_id)
        storage_volumes = [
            {
                "name": make_volume_name_from_path(mount_path),
                "mountPath": mount_path,
                "hostPath": mount_path,
                "readOnly": False,
                "type": "Directory",
            }
        ]
        worker_node_group = [{mount_info["label"]: "true"}]

        task_obj = self._render_template(
            K8S_RM_JOB_TEMPLATE,
            {
                "task_id": task_id,
                "user_id": user_id,
                "job_label": K8S_RM_JOB_LABEL,
                "job_name_prefix": K8S_RM_JOB_NAME_PREFIX,
                "service_image": K8S_RM_JOB_IMAGE,
                "queue_name": queue_name,
                "storage_volumes": storage_volumes,
                "worker_node_group": worker_node_group,
            },
        )

        task_job_name = f"{K8S_RM_JOB_NAME_PREFIX}-{task_id}"
        await self._add_active_job(task_id, task_job_name, "Registered rm job")

        try:
            await self._ensure_task_running(task_id)
            await self.job_runner.create_job(task_obj)

            await self._ensure_task_running(task_id)
            label_selector = f"{K8S_RM_JOB_LABEL}={task_id}"
            rm_pods = await self.job_runner.wait_for_pods_ready(
                label_selector=label_selector,
                expected=1,
                timeout=180,
            )
            pod_name = rm_pods[0].metadata.name

            rm_cmd = self._build_rm_command(target_path, options)
            exec_task = asyncio.create_task(
                self.job_runner.exec_in_pod_with_exit_code(
                    pod_name, ["/bin/bash", "-c", rm_cmd]
                )
            )

            try:
                exec_result = await exec_task
            except asyncio.CancelledError:
                raise
            finally:
                if not exec_task.done():
                    exec_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await exec_task

            await self._record_rm_exit_code(task_id, exec_result)
            await self.job_runner.wait_for_completion(
                label_selector, success_phases=("Succeeded", "Running")
            )
            result = await self._build_task_result(label_selector, pod_name, tail_lines=1000)
        finally:
            try:
                await self._cleanup_job(task_id, task_job_name, "Rm job cleaned up")
            except TaskJobError:
                logger.warning(
                    f"[Task {task_id}] Failed to clean up rm job {task_job_name}"
                )

        await self._ensure_task_running(task_id)
        return result

    async def cancel(self, request: CancelRequest, state: TaskRecord | None = None) -> None:
        task_state = state or await self.state_store.get_task(request.task_id)

        if task_state is None:
            return

        jobs = list(task_state.active_jobs)
        if not jobs:
            await self.state_store.append_log(request.task_id, "No active rm jobs to cancel")
            return

        for job_name in jobs:
            try:
                await self._cleanup_job(
                    request.task_id, job_name, f"Cancellation sent to job {job_name}"
                )
                state = await self.state_store.set_status(
                    request.task_id, TaskStatus.cancelled, "Task cancelled"
                )
                if not state:
                    raise TaskNotFoundError(request.task_id)
            except TaskJobError as exc:
                logger.warning(
                    f"[Task {request.task_id}] Failed to cancel job {job_name}: {exc}"
                )

    async def _verify_mount(
        self, task_id: str, pods: list[V1Pod], mount_path: str
    ) -> None:
        checks: list[PodMountCheckResult] = []

        await self._ensure_task_running(task_id)

        for pod in pods:
            pod_name = pod.metadata.name
            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    ["/bin/bash", "-c", MOUNT_VERIFY_CMD.format(mount_point=mount_path)],
                )
            ).strip() or "__NULL__"
            logger.info(f"[Task {task_id}] 'mount' check of {mount_path} => {output}")
            checks.append(PodMountCheckResult(name=pod_name, output=output))

        if not all(item.output == "__TRUE__" for item in checks):
            raise TaskJobError(task_id, f"Invalid mount point: {checks}")

        await self.state_store.append_log(task_id, "Mount point verification is done")

    async def _verify_pathtype(
        self, task_id: str, pods: list[V1Pod], target_path: str
    ) -> Optional[str]:
        checks: list[PodPathCheckResult] = []

        await self._ensure_task_running(task_id)

        for pod in pods:
            pod_name = pod.metadata.name
            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    ["/bin/bash", "-c", PATHTYPE_VERIFY_CMD.format(target_path=target_path)],
                )
            ).strip() or "__NULL__"
            logger.info(f"[Task {task_id}] target path type => {output}")
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        target_type = checks[0].output if checks else None

        if target_type == "__NOT_FOUND__":
            raise TaskInvalidDirectoryError(task_id, target_path, "Cannot find the target path")
        if target_type not in {"__FILE__", "__DIR__"}:
            raise TaskInvalidDirectoryError(
                task_id, target_path, f"Unknown target path type: {target_type}"
            )

        await self.state_store.append_log(task_id, "Pathtype verification is done")
        return target_type

    async def _verify_ownership(
        self,
        task_id: str,
        user_id: str,
        pods: list[V1Pod],
        target_path: str,
    ) -> Optional[str]:
        checks: list[PodPathCheckResult] = []

        await self._ensure_task_running(task_id)

        for pod in pods:
            pod_name = pod.metadata.name
            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    [
                        "/bin/bash",
                        "-c",
                        RM_OWNERSHIP_VERIFY_CMD.format(
                            user_id=user_id, target_path=target_path
                        ),
                    ],
                )
            ).strip() or "__NULL__"
            logger.info(f"[Task {task_id}] rm ownership => {output}")
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        target_ownership = checks[0].output if checks else None
        if target_ownership != "__TRUE__":
            raise TaskInvalidDirectoryError(
                task_id, target_path, "Permission failed to target path"
            )

        await self.state_store.append_log(task_id, "Ownership verification is done")
        return target_ownership

    async def _ensure_task_running(self, task_id: str) -> None:
        state = await self.state_store.get_task(task_id)
        if state and state.status == TaskStatus.running:
            return

        status_label = getattr(state, "status", "unknown")
        message = f"Task no longer running (status={status_label}); stopping execution"
        await self.state_store.append_log(task_id, message)
        raise TaskJobError(task_id, message)

    async def _get_task_queue_name(self, task_id: str) -> str:
        state = await self.state_store.get_task(task_id)
        priority = getattr(state, "priority", None)

        if priority is None:
            logger.warning("Failed to get a priority of task")
            return K8S_VOLCANO_LOW_PRIO_Q

        if priority == "high":
            return K8S_VOLCANO_HIGH_PRIO_Q

        if priority != "low":
            logger.warning(f"Invalid input of priority: {priority}")

        return K8S_VOLCANO_LOW_PRIO_Q

    def _render_template(self, template_path: str, context: Dict[str, Any]) -> dict:
        try:
            with open(template_path) as f:
                template = Template(f.read())
            rendered = template.render(**context)
            return yaml.safe_load(rendered)
        except Exception as exc:  # pragma: no cover - render failure path
            raise TaskTemplateRenderError(template_path, exc) from exc

    async def _check_format_rm(self, request: TaskRequest) -> None:
        params: Dict[str, Any] = request.parameters or {}
        errors: list[str] = []

        target_path = params.get("path")

        if not isinstance(target_path, str) or not target_path:
            errors.append(f"'path' must be a non-empty string, got {repr(target_path)}")
        elif not target_path.startswith("/"):
            errors.append(f"'path' must be an absolute path starting with '/', got {target_path!r}")
        elif ".." in target_path.split("/"):
            errors.append(f"'path' must not contain '..' segments, got {target_path!r}")

        options = params.get("options")
        if options is not None:
            if not isinstance(options, str):
                errors.append(
                    f"'options' must be a string if provided, got {type(options).__name__}"
                )
            else:
                errors.extend(self._validate_rm_options(options))

        if errors:
            raise TaskInvalidParametersError(request.task_id, request.service, errors)

        if target_path is not None:
            _, target_info = match_allowed_directory(target_path)
            if target_info is None:
                raise TaskInvalidDirectoryError(
                    request.task_id,
                    target_path,
                    f"Invalid path to service '{request.service}'",
                )

    def _validate_rm_options(self, options: str) -> list[str]:
        errors: list[str] = []

        allowed_flags = {
            "-r",
            "-R",
            "--recursive",
            "-f",
            "--force",
            "-v",
            "--verbose",
            "--one-file-system",
        }

        try:
            tokens = shlex.split(options)
        except ValueError as exc:
            errors.append(f"Failed to parse rm options: {exc}")
            return errors

        for token in tokens:
            if token not in allowed_flags:
                errors.append(f"Unsupported rm option: {token!r}")

        return errors

    def _has_recursive_option(self, options: str) -> bool:
        tokens = shlex.split(options) if options else []
        return any(token in {"-r", "-R", "--recursive"} for token in tokens)

    def _build_rm_command(self, target_path: str, options: str) -> str:
        tokens = shlex.split(options) if options else []
        safe_tokens = " ".join(shlex.quote(token) for token in tokens)
        safe_target = shlex.quote(target_path)
        return f"rm {safe_tokens} -- {safe_target}".strip()

    async def _record_rm_exit_code(self, task_id: str, result: ExecResult) -> None:
        exit_code = result.exit_code
        await self.state_store.append_log(
            task_id,
            f"rm exit code: {exit_code if exit_code is not None else 'unknown'} (0 is success)",
        )

        if exit_code not in (None, 0):
            stderr_output = result.stderr.strip()
            message = f"rm failed with exit code {exit_code}"
            if stderr_output:
                message = f"{message} - {stderr_output}"
            raise TaskJobError(task_id, message)

    async def _build_task_result(
        self, label_selector: str, pod_name: str, tail_lines: Optional[int]
    ) -> TaskResult:
        pod_statuses = await self.job_runner.list_pod_statuses(label_selector)
        pod_status_summary = self._summarize_pod_statuses(pod_statuses)
        launcher_output = await self._build_launcher_output(pod_name, tail_lines)
        return TaskResult(pod_status=pod_status_summary, launcher_output=launcher_output)

    async def _build_launcher_output(
        self, pod_name: str, tail_lines: Optional[int]
    ) -> str:
        try:
            raw_logs = await self.job_runner.get_pod_logs(
                pod_name=pod_name, tail_lines=tail_lines
            )
        except TaskJobError as exc:
            logger.warning(f"Failed to read logs from {pod_name}: {exc}")
            raw_logs = ""

        return self._extract_relevant_output(raw_logs, tail_lines=tail_lines)

    def _summarize_pod_statuses(self, pod_statuses: dict[str, str]) -> str:
        if not pod_statuses:
            return "Unknown"
        return " \n".join(f"{name}: {status}" for name, status in pod_statuses.items())

    def _extract_relevant_output(
        self, output: str, tail_lines: Optional[int]
    ) -> str:
        lines = output.splitlines()

        def _is_warning_or_error(line: str) -> bool:
            lower = line.lower()
            return "warn" in lower or "error" in lower or "fail" in lower

        warnings_and_errors = [line for line in lines if _is_warning_or_error(line)]
        truncated = lines[-tail_lines:] if tail_lines and len(lines) > tail_lines else lines

        sections: list[str] = []
        if warnings_and_errors:
            sections.append("[Errors / Warnings]")
            sections.extend(warnings_and_errors)

        sections.append("\n\n[Last lines]")
        sections.extend(truncated)

        return "\n".join(sections)

    async def _cleanup_job(self, task_id: str, job_name: str, message: str) -> bool:
        state = await self.state_store.get_task(task_id)
        if not state or job_name not in state.active_jobs:
            logger.info(
                f"[Task {task_id}] Job {job_name} already cleaned up; skipping cleanup"
            )
            return False

        await self._remove_active_job(task_id, job_name, message)
        try:
            await self.job_runner.delete_job(job_name)
        except TaskJobError:
            await self._add_active_job(
                task_id, job_name, f"Re-added {job_name} after failed cleanup attempt"
            )
            raise

        return True

    async def _add_active_job(self, task_id: str, job_name: str, message: str) -> None:
        updated = await self.state_store.add_active_job(task_id, job_name, message)
        if updated:
            logger.info(f"[Task {task_id}] added active job {job_name}")

    async def _remove_active_job(self, task_id: str, job_name: str, message: str) -> None:
        updated = await self.state_store.remove_active_job(task_id, job_name, message)
        if updated:
            logger.info(f"[Task {task_id}] removed active job {job_name}")


__all__ = ["RmTaskHandler"]
