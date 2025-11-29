import asyncio
import logging
from typing import Any, Dict

from ..models.schemas import TaskRequest, TaskResult, TaskStatus
from .constants import ALLOWED_SERVICES
from .state_store import StateStore

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    async def handle_task(self, request: TaskRequest):
        # get a task state
        state = await self.state_store.get_task(request.task_id)
        if not state:
            raise TaskNotFoundError(request.task_id)

        # check if field values are matching
        mismatches = await self._ensure_request_matches_state(request, state)
        if mismatches:
            raise TaskNotMatchedError(request.task_id, mismatches)

        # service filtering
        if request.service not in ALLOWED_SERVICES:
            raise TaskUnsupportedServiceError(request.task_id, request.service)

        # asynchronously run the task work
        asyncio.create_task(self._run_task(request))
        return state

    async def _run_task(self, request: TaskRequest) -> None:
        task_id = request.task_id
        try:
            logger.info("Dispatching task %s (service %s, parameters %s)", task_id, request.service, request.parameters)
            state = await self.state_store.set_status(task_id, TaskStatus.dispatching, f"Task accepted by scheduler")
            if not state:
                raise TaskNotFoundError(task_id)

            # ----- task execution start -----
            state = await self.state_store.set_status(task_id, TaskStatus.running, "Task started")
            if not state:
                raise TaskNotFoundError(task_id)

            # Dummy execeution
            await asyncio.sleep(0.1)


            # ----- task execution end -----



            # --------------------------------------------------
            # 아래 내용은 실제 task executor 코드에서 진행되어야 함.
            result = TaskResult(
                pod_status="Succeeded",
                launcher_output=f"Handled parameters: {request.parameters}",
            )
            updated = await self.state_store.set_result(
                task_id, result, "Task finished successfully"
            )
            if not updated:
                raise TaskNotFoundError(task_id)

            state = await self.state_store.set_status(task_id, TaskStatus.completed, "Task completed")
            if not state:
                raise TaskNotFoundError(task_id)

            # --------------------------------------------------

        except asyncio.CancelledError:
            await self.state_store.set_status(
                task_id,
                TaskStatus.cancelled,
                "Task cancelled during async execution",
            )
        except Exception as exc:
            logger.exception("Task %s failed: %s", task_id, exc)
            await self.state_store.set_status(
                task_id, TaskStatus.failed, f"Unexpected Error: {exc}"
            )

    async def _ensure_request_matches_state(self, request: TaskRequest, state: Any) -> Dict[str, tuple[Any, Any]]:
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


    async def cancel_task(self, task_id: str):
        # TODO: user_id 매치하는지 확인해야 함.
        # TODO: running 일떄만 cancel success, 나머지는 cancel 실패 및 status 반환

        state = await self.state_store.set_status(
            task_id, TaskStatus.cancel_requested, "Cancellation requested"
        )
        if not state:
            raise TaskNotFoundError(task_id)
        return state


class TaskNotFoundError(Exception):
    """Raised when a task_id does not exist in the shared store."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        message = f"Task {task_id} not found in shared store"

        logger.error(message)
        super().__init__(message)


class TaskNotMatchedError(Exception):
    """Raised when request fields do not match the stored task state."""

    def __init__(self, task_id: str, mismatches: Dict[str, tuple[Any, Any]]):
        self.task_id = task_id
        self.mismatches = mismatches

        details = []
        fields = []
        for field, (req_val, stored_val) in mismatches.items():
            details.append(f"Field <{field}> mismatch: request={req_val!r}, stored={stored_val!r}")
            fields.append(field)
        message = f"Task {task_id} request does not match stored task. " + "; ".join(details)

        logger.error(message)
        super().__init__(message)


class TaskUnsupportedServiceError(Exception):
    """Raised when the requested service is not supported by the executor."""

    def __init__(self, task_id: str, service: str):
        self.task_id = task_id
        self.service = service

        message = f"Task {task_id} requested unsupported service: {service!r}"

        logger.error(message)
        super().__init__(message)

