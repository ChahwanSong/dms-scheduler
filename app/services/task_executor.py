import asyncio
import logging

from ..models.schemas import TaskRequest, TaskResult, TaskStatus
from .state_store import StateStore

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    async def handle_task(self, request: TaskRequest):
        state = await self.state_store.set_status(
            request.task_id, TaskStatus.dispatching, "Task accepted and queued"
        )
        if not state:
            raise TaskNotFoundError(request.task_id)

        asyncio.create_task(self._run_task(request))
        return state

    async def _run_task(self, request: TaskRequest) -> None:
        task_id = request.task_id
        try:
            state = await self.state_store.set_status(task_id, TaskStatus.running, "Task started")
            if not state:
                raise TaskNotFoundError(task_id)
            logger.info("Executing task %s with payload %s", task_id, request.parameters)
            await asyncio.sleep(0.1)
            result = TaskResult(
                pod_status="Succeeded",
                launcher_output=f"Handled parameters: {request.parameters}",
            )
            await self.state_store.set_result(task_id, result, "Task finished successfully")
            await self.state_store.set_status(task_id, TaskStatus.completed)
        except asyncio.CancelledError:
            await self.state_store.set_status(task_id, TaskStatus.cancelled, "Task cancelled during execution")
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Task %s failed: %s", task_id, exc)
            await self.state_store.set_status(task_id, TaskStatus.failed, f"Error: {exc}")

    async def cancel_task(self, task_id: str):
        state = await self.state_store.set_status(task_id, TaskStatus.cancel_requested, "Cancellation requested")
        if not state:
            raise TaskNotFoundError(task_id)
        return state


class TaskNotFoundError(Exception):
    """Raised when a task_id does not exist in the shared store."""

    def __init__(self, task_id: str):
        super().__init__(f"Task {task_id} not found in shared store")
        self.task_id = task_id
