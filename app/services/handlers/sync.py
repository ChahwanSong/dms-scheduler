"""Sync task handler implementation."""

import os
import asyncio
import logging
import pwd
import shlex
from contextlib import suppress
from typing import Any, Dict, Optional, Tuple

import yaml
from jinja2 import Template
from kubernetes.client import V1Pod

from ..constants import (
    K8S_DMS_LOG_DIRECTORY,
    K8S_SYNC_D_JOB_TEMPLATE,
    K8S_SYNC_D_JOB_IMAGE,
    K8S_SYNC_D_WORKER_HOSTFILE_PATH,
    K8S_SYNC_D_DEFAULT_N_BATCH_FILES,
    K8S_SYNC_D_DEFAULT_N_CPU_PER_WORKER,
    K8S_SYNC_D_DEFAULT_N_WORKERS,
    K8S_SYNC_D_DEFAULT_MASTER_N_CPU,
    K8S_SYNC_D_DEFAULT_MASTER_MEMORY,
    K8S_SYNC_D_DEFAULT_WORKER_MEMORY,
    K8S_SYNC_LOG_TAIL_LINES,
    K8S_SYNC_PROGRESS_UPDATE_INTERVAL,
    K8S_SYNC_VERIFIER_TEMPLATE,
    K8S_SYNC_VERIFIER_JOB_LABEL,
    K8S_SYNC_VERIFIER_JOB_NAME_PREFIX,
    K8S_SYNC_VERIFIER_JOB_IMAGE,
    K8S_SYNC_JOB_LABEL,
    K8S_SYNC_JOB_NAME_PREFIX,
    K8S_VOLCANO_HIGH_PRIO_Q,
    K8S_VOLCANO_LOW_PRIO_Q,
)
from ..cmds import (
    MOUNT_VERIFY_CMD,
    PATHTYPE_VERIFY_CMD,
    SYNC_OWNERSHIP_VERIFY_SRC_FILE_CMD,
    SYNC_OWNERSHIP_VERIFY_SRC_DIR_CMD,
    SYNC_OWNERSHIP_VERIFY_DST_CMD,
    DSYNC_RUN_CMD,
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
from ...models.schemas import (
    CancelRequest,
    TaskRecord,
    TaskRequest,
    TaskResult,
    TaskStatus,
)
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
        task_id = request.task_id
        user_id = request.user_id
        pwd.getpwnam(user_id)

        params = request.parameters or {}
        src: str = params.get("src")
        dst: str = params.get("dst")
        options = params.get("options") or ""

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

        # ------------------- VERIFICATION -------------------
        await self._ensure_task_running(task_id)
        verifier_obj = self._render_template(
            K8S_SYNC_VERIFIER_TEMPLATE,
            {
                "task_id": task_id,
                "job_name_prefix": K8S_SYNC_VERIFIER_JOB_NAME_PREFIX,
                "verifier_job_label": K8S_SYNC_VERIFIER_JOB_LABEL,
                "queue_name": K8S_VOLCANO_HIGH_PRIO_Q,
                "verifier_image": K8S_SYNC_VERIFIER_JOB_IMAGE,
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

        verifier_job_name = f"{K8S_SYNC_VERIFIER_JOB_NAME_PREFIX}-{task_id}"
        await self._add_active_job(
            task_id, verifier_job_name, "Registered verifier job"
        )

        try:
            await self._ensure_task_running(task_id)
            await self.job_runner.create_job(verifier_obj)

            await self._ensure_task_running(task_id)
            verifier_pods = await self.job_runner.wait_for_pods_ready(
                label_selector=f"{K8S_SYNC_VERIFIER_JOB_LABEL}={task_id}",
                expected=2,
                timeout=180,
            )

            await self._verify_mount(
                task_id, verifier_pods, src_mount_path, dst_mount_path
            )
            src_path_type, dst_path_type = await self._verify_pathtype(
                task_id, verifier_pods, src, dst
            )

            if user_id != "root":
                await self._verify_ownership(
                    task_id,
                    user_id,
                    verifier_pods,
                    src,
                    src_path_type,
                    dst,
                    dst_path_type,
                )

            _temp = """TEST_CMD = "while true; do date '+%Y-%m-%d %H:%M:%S'; sleep 1; done"
            await self._ensure_task_running(task_id)
            logger.info(f"Run infinite loop on {verifier_pods[0].metadata.name}")
            await self.job_runner.exec_in_pod(verifier_pods[0].metadata.name,
                    ["/bin/bash", "-c", TEST_CMD]) """
        finally:
            try:
                await self._cleanup_job(
                    task_id, verifier_job_name, "Verifier job cleaned up"
                )
            except TaskJobError:
                logger.warning(
                    f"[Task {task_id}] Failed to clean up verifier job {verifier_job_name}"
                )

        # ------------------- TASK RUNNING -------------------
        # TODO: operation type 정하기 (dsync, nsync)
        # src, dst 경로가 같은 mount_path 인지, 또는 mount_path 가 nsync 인지 dsync 로
        # 가능한지 체크 로직 추가
        op_type = "dsync"

        # TODO: sync 파라미터
        master_node_group = [
            {src_info["label"]: "true"},  # src node 중에 마스터 할당
        ]
        worker_node_group = [
            {src_info["label"]: "true"},
            {dst_info["label"]: "true"},
        ]
        storage_volumes = [
            {
                "name": make_volume_name_from_path(p),
                "mountPath": p,
                "hostPath": p,
                "readOnly": False,
                "type": "Directory",
            }
            for p in set([src_mount_path, dst_mount_path])
        ]
        queue_name = await self._get_task_queue_name(task_id)

        if op_type == "dsync":
            task_obj = self._render_template(
                K8S_SYNC_D_JOB_TEMPLATE,
                {
                    "task_id": task_id,
                    "user_id": user_id,
                    "job_label": K8S_SYNC_JOB_LABEL,
                    "job_name_prefix": K8S_SYNC_JOB_NAME_PREFIX,
                    "service_image": K8S_SYNC_D_JOB_IMAGE,
                    "n_workers": int(K8S_SYNC_D_DEFAULT_N_WORKERS),
                    "queue_name": queue_name,
                    "storage_volumes": storage_volumes,
                    "master_node_group": master_node_group,
                    "master_n_cpu": int(K8S_SYNC_D_DEFAULT_MASTER_N_CPU),
                    "master_memory": K8S_SYNC_D_DEFAULT_MASTER_MEMORY,
                    "worker_node_group": worker_node_group,
                    "worker_n_cpu": int(K8S_SYNC_D_DEFAULT_N_CPU_PER_WORKER),
                    "worker_memory": K8S_SYNC_D_DEFAULT_WORKER_MEMORY,
                },
            )

            task_job_name = f"{K8S_SYNC_JOB_NAME_PREFIX}-{task_id}"
            await self._add_active_job(
                task_id, task_job_name, f"Registered sync job - {op_type}"
            )

            try:
                await self._ensure_task_running(task_id)
                await self.job_runner.create_job(task_obj)

                await self._ensure_task_running(task_id)
                label_selector = f"{K8S_SYNC_JOB_LABEL}={task_id}"
                sync_pods = await self.job_runner.wait_for_pods_ready(
                    label_selector=label_selector,
                    expected=2,
                    timeout=180,
                )
                pod_name = sync_pods[0].metadata.name

                _temp = """TEST_CMD = "while true; do date '+%Y-%m-%d %H:%M:%S'; sleep 1; done"
                await self._ensure_task_running(task_id)
                logger.info(f"Run infinite loop on {sync_pods[0].metadata.name}")
                await self.job_runner.exec_in_pod(sync_pods[0].metadata.name,
                        ["/bin/bash", "-c", TEST_CMD])"""

                result = await self._run_dsync(
                    task_id=task_id,
                    label_selector=label_selector,
                    pod_name=pod_name,
                    src_path=src,
                    dst_path=dst,
                    options=options,
                )

            finally:
                try:
                    await self._cleanup_job(
                        task_id, task_job_name, f"{op_type} job cleaned up"
                    )
                except TaskJobError:
                    logger.warning(
                        f"[Task {task_id}] Failed to clean up verifier job {task_job_name}"
                    )

        await self._ensure_task_running(task_id)
        return result

    async def cancel(
        self, request: CancelRequest, state: TaskRecord | None = None
    ) -> None:
        task_state = state
        if task_state is None:
            task_state = await self.state_store.get_task(request.task_id)

        if task_state is None:
            return

        jobs = list(task_state.active_jobs)
        if not jobs:
            await self.state_store.append_log(
                request.task_id, "No active jobs to cancel"
            )
            return

        for job_name in jobs:
            try:
                await self._cleanup_job(
                    request.task_id, job_name, f"Cancellation sent to job {job_name}"
                )
                state = await self.state_store.set_status(
                    request.task_id, TaskStatus.cancelled, f"Task cancelled"
                )
                if not state:
                    raise TaskNotFoundError(request.task_id)
            except TaskJobError as exc:
                logger.warning(
                    f"[Task {request.task_id}] Failed to cancel job {job_name}: {exc}"
                )

    async def _verify_mount(
        self, task_id: str, pods: list[V1Pod], src_mount_path: str, dst_mount_path: str
    ) -> None:
        checks: list[PodMountCheckResult] = []

        await self._ensure_task_running(task_id)

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
                    [
                        "/bin/bash",
                        "-c",
                        MOUNT_VERIFY_CMD.format(mount_point=mount_point),
                    ],
                )
            ).strip() or "__NULL__"
            logger.info(f"[Task {task_id}] 'mount' check of {mount_point} => {output}")
            checks.append(PodMountCheckResult(name=pod_name, output=output))

        is_mount_verified = all(item.output == "__TRUE__" for item in checks)
        if not is_mount_verified:
            raise TaskJobError(task_id, f"Invalid mount point: {checks}")

        # write a log
        await self.state_store.append_log(task_id, "Mount point verification is done")

    async def _verify_pathtype(
        self, task_id: str, pods: list[V1Pod], src_path: str, dst_path: str
    ) -> Tuple[Optional[str], Optional[str]]:
        checks: list[PodPathCheckResult] = []

        await self._ensure_task_running(task_id)

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
                    [
                        "/bin/bash",
                        "-c",
                        PATHTYPE_VERIFY_CMD.format(target_path=target_path),
                    ],
                )
            ).strip() or "__NULL__"
            if type_path == "src":
                logger.info(f"[Task {task_id}] src type => {output}")

            if type_path == "dst":
                logger.info(f"[Task {task_id}] dst type => {output}")
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        src_path_type = next(
            (r.output for r in checks if "src-checker" in r.name), None
        )
        dst_path_type = next(
            (r.output for r in checks if "dst-checker" in r.name), None
        )

        if src_path_type == "__NOT_FOUND__":
            raise TaskInvalidDirectoryError(
                task_id, src_path, "Cannot find the src path"
            )
        elif src_path_type != "__FILE__" and src_path_type != "__DIR__":
            raise TaskInvalidDirectoryError(
                task_id, src_path, f"Unknown src path type: {src_path_type}"
            )

        if dst_path_type != "__DIR__":
            raise TaskInvalidDirectoryError(
                task_id, dst_path, f"Dst path is not a directory - {dst_path_type}"
            )

        # write a log
        await self.state_store.append_log(task_id, "Pathtype verification is done")

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

        await self._ensure_task_running(task_id)

        for pod in pods:
            pod_name = pod.metadata.name
            if "src-checker" in pod_name:
                target_path = src_path
                if src_path_type == "__FILE__":
                    target_cmd = SYNC_OWNERSHIP_VERIFY_SRC_FILE_CMD
                else:
                    target_cmd = SYNC_OWNERSHIP_VERIFY_SRC_DIR_CMD
                type_path = "src"
            elif "dst-checker" in pod_name:
                target_path = dst_path
                target_cmd = SYNC_OWNERSHIP_VERIFY_DST_CMD
                type_path = "dst"
            else:
                raise TaskJobError(pod_name, "Verifier pod naming is invalid")

            output = (
                await self.job_runner.exec_in_pod(
                    pod_name,
                    [
                        "/bin/bash",
                        "-c",
                        target_cmd.format(user_id=user_id, target_path=target_path),
                    ],
                )
            ).strip() or "__NULL__"
            logger.info(f"[Task {task_id}] {type_path} ownership => {output}")
            checks.append(PodPathCheckResult(name=pod_name, output=output))

        src_ownership = next(
            (r.output for r in checks if "src-checker" in r.name), None
        )
        if src_ownership != "__TRUE__":
            raise TaskInvalidDirectoryError(
                task_id, src_path, f"Permission failed to src path"
            )

        dst_ownership = next(
            (r.output for r in checks if "dst-checker" in r.name), None
        )
        if dst_ownership != "__TRUE__":
            raise TaskInvalidDirectoryError(
                task_id, dst_path, f"Permission failed to dst path"
            )

        # write a log
        await self.state_store.append_log(task_id, "Ownership verification is done")

        return src_ownership, dst_ownership

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

        # default, return a low priority
        return K8S_VOLCANO_LOW_PRIO_Q

    def _render_template(self, template_path: str, context: Dict[str, Any]) -> dict:
        try:
            with open(template_path) as f:
                template = Template(f.read())
            rendered = template.render(**context)
            return yaml.safe_load(rendered)
        except Exception as exc:  # pragma: no cover - render failure path
            raise TaskTemplateRenderError(template_path, exc) from exc

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
            errors.append(
                f"'src' must be an absolute path starting with '/', got {src!r}"
            )
        if isinstance(dst, str) and not dst.startswith("/"):
            errors.append(
                f"'dst' must be an absolute path starting with '/', got {dst!r}"
            )

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

    async def _run_dsync(
        self,
        task_id: str,
        label_selector: str,
        pod_name: str,
        src_path: str,
        dst_path: str,
        options: str,
    ) -> TaskResult:

        await self._ensure_task_running(task_id)

        tokens = self._build_dsync_tokens(options)

        dsync_cmd = DSYNC_RUN_CMD.format(
            n_slots_per_host=int(K8S_SYNC_D_DEFAULT_N_CPU_PER_WORKER),
            worker_hostfile=K8S_SYNC_D_WORKER_HOSTFILE_PATH,
            options=" ".join(shlex.quote(token) for token in tokens),
            src_path=src_path,
            dst_path=dst_path,
        )

        dsync_task = asyncio.create_task(
            self.job_runner.exec_in_pod_with_exit_code(
                pod_name, ["/bin/bash", "-c", dsync_cmd]
            )
        )
        task_result: TaskResult | None = None

        try:
            while True:
                done, _ = await asyncio.wait(
                    {dsync_task}, timeout=int(K8S_SYNC_PROGRESS_UPDATE_INTERVAL)
                )
                progress_result = await self._update_task_progress(
                    task_id, label_selector, pod_name
                )
                if progress_result is not None:
                    task_result = progress_result
                if dsync_task in done:
                    break

            dsync_result = await dsync_task

            await self._record_dsync_exit_code(task_id, dsync_result)
            await self.job_runner.wait_for_completion(
                label_selector, success_phases=("Succeeded", "Running")
            )
            task_result = await self._build_task_result(
                label_selector, pod_name, tail_lines=10000
            )
            logger.info(f"[Task {task_id}] Task finished")

        except asyncio.CancelledError:
            raise
        except TaskJobError as exc:
            await self.state_store.append_log(task_id, f"dsync execution failed: {exc}")
            raise
        except Exception as exc:
            await self.state_store.append_log(
                task_id, f"dsync execution failed unexpectedly: {exc}"
            )
            raise TaskJobError(task_id, f"Unexpected dsync failure: {exc}") from exc
        else:
            await self.state_store.append_log(task_id, "dsync execution completed")
        finally:
            ### always run the below code
            # enforce to finish
            if not dsync_task.done():
                dsync_task.cancel()
                with suppress(asyncio.CancelledError):
                    await dsync_task

            if task_result is None:
                with suppress(Exception):
                    task_result = await self._build_task_result(
                        label_selector=label_selector,
                        pod_name=pod_name,
                        tail_lines=10000,
                    )

            if task_result is not None:
                with suppress(Exception):
                    await self.state_store.set_result(task_id, task_result)

                # enforce to save a log file
                await self._save_dsync_log_file(
                    task_id, task_result.launcher_output or ""
                )
                task_result.launcher_output = self._tail_output(
                    task_result.launcher_output or "",
                    int(K8S_SYNC_LOG_TAIL_LINES),
                )

        # return a log to memorize
        if task_result is None:
            return TaskResult(pod_status="Unknown", launcher_output="")
        return task_result

    async def _record_dsync_exit_code(self, task_id: str, result: ExecResult) -> None:
        exit_code = result.exit_code
        await self.state_store.append_log(
            task_id,
            f"dsync exit code: {exit_code if exit_code is not None else 'unknown'} (0 is success)",
        )

        if exit_code not in (None, 0):
            stderr_output = result.stderr.strip()
            message = f"dsync failed with exit code {exit_code}"
            if stderr_output:
                message = f"{message} - {stderr_output}"
            raise TaskJobError(task_id, message)

    async def _update_task_progress(
        self,
        task_id: str,
        label_selector: str,
        pod_name: str,
        tail_lines: Optional[int] = None,
    ) -> TaskResult | None:
        await self._ensure_task_running(task_id)

        try:
            task_result = await self._build_task_result(
                label_selector, pod_name, tail_lines
            )
        except TaskJobError as exc:
            logger.warning(f"[Task {task_id}] Failed to build launcher output: {exc}")
            return None

        await self.state_store.set_result(task_id, task_result)
        return task_result

    async def _save_dsync_log_file(self, task_id: str, output: str) -> None:
        dir_path = K8S_DMS_LOG_DIRECTORY
        log_path = os.path.join(dir_path, f"{task_id}.log")
        try:
            os.makedirs(dir_path, exist_ok=True)
            with open(log_path, "w", encoding="utf-8") as f:
                f.write(output)
        except OSError as exc:
            logger.error(f"[Task {task_id}] Failed to save a log: {log_path} ({exc})")
            with suppress(Exception):
                await self.state_store.append_log(
                    task_id, f"Failed to save dsync log file: {log_path}"
                )
        else:
            logger.info(f"[Task {task_id}] Saved a log: {log_path}")
            with suppress(Exception):
                await self.state_store.append_log(
                    task_id, f"Saved dsync log file: {log_path}"
                )

    def _tail_output(self, output: str, tail_lines: int) -> str:
        if tail_lines <= 0:
            return output

        lines = output.splitlines()
        return "\n".join(lines[-tail_lines:])

    async def _build_task_result(
        self, label_selector: str, pod_name: str, tail_lines: Optional[int]
    ) -> TaskResult:
        pod_statuses = await self.job_runner.list_pod_statuses(label_selector)
        pod_status_summary = self._summarize_pod_statuses(pod_statuses)

        launcher_output = await self._build_launcher_output(pod_name, tail_lines)

        return TaskResult(
            pod_status=pod_status_summary, launcher_output=launcher_output
        )

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

        relevant_output = self._extract_relevant_output(raw_logs, tail_lines=tail_lines)
        return relevant_output

    def _summarize_pod_statuses(self, pod_statuses: dict[str, str]) -> str:
        if not pod_statuses:
            return "Unknown"
        return " \n".join(f"{name}: {status}" for name, status in pod_statuses.items())

    def _extract_relevant_output(self, output: str, tail_lines: Optional[int]) -> str:
        lines = output.splitlines()

        def _is_warning_or_error(line: str) -> bool:
            lower = line.lower()
            return "warn" in lower or "error" in lower or "fail" in lower

        warnings_and_errors = [line for line in lines if _is_warning_or_error(line)]
        if tail_lines is None:
            truncated = lines
        else:
            truncated = lines[-tail_lines:] if len(lines) > tail_lines else lines

        sections: list[str] = []
        if warnings_and_errors:
            sections.append("[Errors / Warnings]")
            sections.extend(warnings_and_errors)

        sections.append(f"\n\n[Last lines]")
        sections.extend(truncated)

        return "\n".join(sections)

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
                        errors.append(
                            f"Option {flag!r} must be an integer, got {value!r}"
                        )
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

    def _build_dsync_tokens(self, options: str) -> list[str]:
        tokens = shlex.split(options) if options else []

        if "--batch-files" not in tokens:
            tokens.extend(["--batch-files", str(K8S_SYNC_D_DEFAULT_N_BATCH_FILES)])
        if "--direct" not in tokens:
            tokens.append("--direct")
        if "--open-noatime" not in tokens:
            tokens.append("--open-noatime")

        return tokens

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

    async def _remove_active_job(
        self, task_id: str, job_name: str, message: str
    ) -> None:
        updated = await self.state_store.remove_active_job(task_id, job_name, message)
        if updated:
            logger.info(f"[Task {task_id}] removed active job {job_name}")


__all__ = ["SyncTaskHandler"]
