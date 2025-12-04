"""Task executor wiring service handlers and state transitions."""

import asyncio
import logging
from typing import Any, Dict

from .constants import ALLOWED_SERVICES, K8S_DMS_NAMESPACE
from .errors import (
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskJobError,
    TaskNotFoundError,
    TaskNotMatchedError,
    TaskTemplateRenderError,
    TaskUnsupportedServiceError,
)
from .handlers.base import BaseTaskHandler
from .handlers.sync import SyncTaskHandler
from .kube import VolcanoJobRunner
from ..models.schemas import TaskRequest, TaskResult, TaskStatus
from .state_store import StateStore

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store
        job_runner = VolcanoJobRunner(K8S_DMS_NAMESPACE)
        self._handlers: Dict[str, BaseTaskHandler] = {
            "sync": SyncTaskHandler(job_runner),
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
            logger.error("Task %s failed: %s", request.task_id, exc)
            await self._transition(request.task_id, TaskStatus.failed, str(exc))
            raise

        asyncio.create_task(self._run_task(request, handler))
        return state

    async def _run_task(self, request: TaskRequest, handler: BaseTaskHandler) -> None:
        task_id = request.task_id
        try:
            await self._transition(task_id, TaskStatus.dispatching, "Task accepted by scheduler")
            
            result: TaskResult = await handler.execute(request)

            await self._transition(task_id, TaskStatus.running, "Task started")
            
            # after confirming the job completion
            updated = await self.state_store.set_result(task_id, result, "Task finished successfully")
            if not updated:
                raise TaskNotFoundError(task_id)

            await self._transition(task_id, TaskStatus.completed, "Task completed")
        except asyncio.CancelledError:
            await self._transition(task_id, TaskStatus.cancelled, "Task cancelled")
        except (
            TaskInvalidParametersError,
            TaskInvalidDirectoryError,
            TaskTemplateRenderError,
            TaskJobError,
        ) as exc:
            logger.error("Task %s failed: %s", task_id, exc)
            await self._transition(task_id, TaskStatus.failed, str(exc))
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Task %s failed unexpectedly", task_id)
            await self._transition(task_id, TaskStatus.failed, f"Unexpected error: {exc}")

    async def cancel_task(self, task_id: str):
        state = await self.state_store.set_status(
            task_id, TaskStatus.cancel_requested, "Cancellation requested"
        )
        if not state:
            raise TaskNotFoundError(task_id)
        return state

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
