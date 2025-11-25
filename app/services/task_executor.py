import asyncio
import logging

from ..models.schemas import TaskRequest, TaskResult, TaskState, TaskStatus
from .state_store import StateStore

logger = logging.getLogger(__name__)


class TaskExecutor:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    async def handle_task(self, request: TaskRequest) -> TaskState:
        state = TaskState(
            task_id=request.task_id,
            service=request.service,
            user_id=request.user_id,
            parameters=request.parameters,
            status=TaskStatus.accepted,
        )
        await self.state_store.save_task(state)
        await self.state_store.append_log(request.task_id, "Task accepted and queued")
        asyncio.create_task(self._run_task(request))
        return state

    async def _run_task(self, request: TaskRequest) -> None:
        task_id = request.task_id
        try:
            await self.state_store.set_status(task_id, TaskStatus.running, "Task started")
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

    async def cancel_task(self, task_id: str) -> TaskState | None:
        return await self.state_store.set_status(task_id, TaskStatus.cancelled, "Cancellation requested")
