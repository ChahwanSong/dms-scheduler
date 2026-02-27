from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.models.schemas import TaskRequest, TaskStatus
from app.services.errors import TaskJobError
from app.services.task_executor import TaskExecutor


class _DummyStateStore:
    def __init__(self, task_state):
        self.task_state = task_state
        self.set_status = AsyncMock(return_value=task_state)
        self.set_result = AsyncMock(return_value=task_state)

    async def get_task(self, task_id: str):
        return self.task_state


@pytest.mark.anyio
async def test_run_task_logs_info_when_task_job_error_after_cancellation(caplog):
    task_state = SimpleNamespace(status=TaskStatus.cancelled)
    state_store = _DummyStateStore(task_state)
    executor = TaskExecutor(state_store=state_store)

    handler = AsyncMock()
    handler.execute = AsyncMock(side_effect=TaskJobError("t1", "cancelled by request"))

    request = TaskRequest(task_id="t1", service="sync", user_id="u1", parameters={})

    caplog.set_level("INFO")
    await executor._run_task(request, handler)

    assert "stopped due to cancellation" in caplog.text
    assert "Task t1 failed" not in caplog.text
    assert state_store.set_status.await_count == 2
    statuses = [call.args[1] for call in state_store.set_status.await_args_list]
    assert statuses == [TaskStatus.dispatching, TaskStatus.running]


@pytest.mark.anyio
async def test_run_task_keeps_error_for_non_cancelled_task_job_error(caplog):
    task_state = SimpleNamespace(status=TaskStatus.running)
    state_store = _DummyStateStore(task_state)
    executor = TaskExecutor(state_store=state_store)

    handler = AsyncMock()
    handler.execute = AsyncMock(side_effect=TaskJobError("t1", "pod startup failure"))

    request = TaskRequest(task_id="t1", service="sync", user_id="u1", parameters={})

    caplog.set_level("ERROR")
    await executor._run_task(request, handler)

    assert "Task t1 failed: pod startup failure" in caplog.text
    statuses = [call.args[1] for call in state_store.set_status.await_args_list]
    assert statuses == [TaskStatus.dispatching, TaskStatus.running, TaskStatus.failed]
