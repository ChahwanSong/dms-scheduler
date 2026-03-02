"""Task executor wiring service handlers and state transitions."""

import asyncio
import logging
from typing import Any, Dict

from .constants import ALLOWED_SERVICES, K8S_DMS_NAMESPACE
from .errors import (
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskJobError,
    TaskCancelForbiddenError,
    TaskNotFoundError,
    TaskNotMatchedError,
    TaskTemplateRenderError,
    TaskUnsupportedServiceError,
)
from .handlers.base import BaseTaskHandler
from .handlers.hotcold import HotcoldTaskHandler
from .handlers.rm import RmTaskHandler
from .handlers.sync import SyncTaskHandler
from .kube import VolcanoJobRunner
from ..models.schemas import CancelRequest, TaskRequest, TaskResult, TaskStatus
from .state_store import StateStore

logger = logging.getLogger(__name__)

_CANCEL_PROPAGATION_TIMEOUT_SECONDS = 2.0


class TaskExecutor:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store
        self._running_tasks: Dict[str, asyncio.Task[None]] = {}
        job_runner = VolcanoJobRunner(K8S_DMS_NAMESPACE)
        self._handlers: Dict[str, BaseTaskHandler] = {
            "sync": SyncTaskHandler(job_runner, state_store),
            "rm": RmTaskHandler(job_runner, state_store),
            "hotcold": HotcoldTaskHandler(job_runner, state_store),
        }

    async def handle_task(self, request: TaskRequest):
        state = await self.state_store.get_task(request.task_id)
        if not state:
            raise TaskNotFoundError(request.task_id)

        mismatches = await self._ensure_request_matches_state(request, state)
        if mismatches:
            details = []
            for field, (req_val, stored_val) in mismatches.items():
                details.append(f"Field <{field}> mismatch: request={req_val!r}, stored={stored_val!r}")
            msg = "Request does not match stored task. " + "; ".join(details)
            logger.error(f"Task {request.task_id} failed: {msg}")
            await self._transition(request.task_id, TaskStatus.failed, msg)
            raise TaskNotMatchedError(request.task_id, msg)

        if request.service not in ALLOWED_SERVICES:
            msg = f"Requested unsupported service: {request.service}"
            logger.error(f"Task {request.task_id} failed: {msg}")
            await self._transition(request.task_id, TaskStatus.failed, msg)
            raise TaskUnsupportedServiceError(request.task_id, msg)

        handler = self._handlers.get(request.service)
        if not handler:
            msg = f"Scheduler has no handler of service: {request.service}"
            logger.error(f"Task {request.task_id} failed: {msg}")
            await self._transition(request.task_id, TaskStatus.failed, msg)
            raise TaskUnsupportedServiceError(request.task_id, msg)

        try:
            await handler.validate(request)
        except (TaskInvalidParametersError, TaskInvalidDirectoryError) as exc:
            logger.error(f"Task {request.task_id} failed: {exc}")
            await self._transition(request.task_id, TaskStatus.failed, str(exc))
            raise

        running_task = asyncio.create_task(self._run_task(request, handler))
        self._running_tasks[request.task_id] = running_task

        def _cleanup(done_task: asyncio.Task[None], task_id: str = request.task_id) -> None:
            if self._running_tasks.get(task_id) is done_task:
                self._running_tasks.pop(task_id, None)

        running_task.add_done_callback(_cleanup)
        return state

    async def _run_task(self, request: TaskRequest, handler: BaseTaskHandler) -> None:
        task_id = request.task_id
        try:
            await self._transition(task_id, TaskStatus.dispatching, "Task accepted by scheduler")

            await self._transition(task_id, TaskStatus.running, "Task started")

            result: TaskResult = await handler.execute(request)

            # after confirming the job completion
            updated = await self.state_store.set_result(task_id, result, "Task result updated")
            if not updated:
                raise TaskNotFoundError(task_id)

            await self._transition(task_id, TaskStatus.completed, "Task completed")
        except asyncio.CancelledError:
            await self._transition(
                task_id,
                TaskStatus.cancelled,
                "Task cancelled abnormally by asyncio",
            )
        except (
            TaskInvalidParametersError,
            TaskInvalidDirectoryError,
            TaskTemplateRenderError,
            TaskJobError,
        ) as exc:
            state = await self.state_store.get_task(task_id)
            if state and state.status in (TaskStatus.cancel_requested, TaskStatus.cancelled):
                logger.info("Task %s stopped due to cancellation: %s", task_id, exc)
            else:
                logger.error(f"Task {task_id} failed: {exc}")
                await self._transition(task_id, TaskStatus.failed, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(f"Task {task_id} failed unexpectedly")
            await self._transition(task_id, TaskStatus.failed, f"Unexpected error: {exc}")

    async def cancel_task(self, request: CancelRequest):
        state = await self.state_store.get_task(request.task_id)
        if not state:
            raise TaskNotFoundError(request.task_id)

        if state.service != request.service or state.user_id != request.user_id:
            msg = "Cancel request does not match stored task identity"
            raise TaskCancelForbiddenError(request.task_id, msg)

        handler = self._handlers.get(request.service)
        if not handler:
            msg = f"Scheduler has no handler of service: {request.service}"
            raise TaskUnsupportedServiceError(request.task_id, msg)

        updated = await self._transition(
            request.task_id, TaskStatus.cancel_requested, "Cancellation requested at scheduler"
        )

        running_task = self._running_tasks.get(request.task_id)
        if running_task and not running_task.done():
            running_task.cancel()
            logger.info("task_id=%s cancel signal delivered", request.task_id)
            try:
                await asyncio.wait_for(running_task, timeout=_CANCEL_PROPAGATION_TIMEOUT_SECONDS)
            except asyncio.CancelledError:
                logger.info("task_id=%s cancel signal delivered and acknowledged", request.task_id)
            except asyncio.TimeoutError:
                logger.warning("task_id=%s propagation timeout during cancel wait", request.task_id)
            finally:
                if running_task.done() and self._running_tasks.get(request.task_id) is running_task:
                    self._running_tasks.pop(request.task_id, None)

        await handler.cancel(request, updated)
        return await self.state_store.get_task(request.task_id)

    async def _transition(self, task_id: str, status: TaskStatus, message: str):
        state = await self.state_store.set_status(task_id, status, message)
        if not state:
            raise TaskNotFoundError(task_id)
        return state

    @staticmethod
    async def _ensure_request_matches_state(
        request: TaskRequest, state: Any
    ) -> Dict[str, tuple[Any, Any]]:
        mismatches: Dict[str, tuple[Any, Any]] = {}

        if getattr(state, "service", None) != request.service:
            mismatches["service"] = (request.service, getattr(state, "service", None))

        if getattr(state, "user_id", None) != request.user_id:
            mismatches["user_id"] = (request.user_id, getattr(state, "user_id", None))

        if getattr(state, "parameters", None) != request.parameters:
            mismatches["parameters"] = (
                request.parameters,
                getattr(state, "parameters", None),
            )

        return mismatches


__all__ = ["TaskExecutor"]
