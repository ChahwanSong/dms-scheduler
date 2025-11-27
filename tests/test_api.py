import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi import HTTPException

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from app.api import deps
from app.api.admin import block_all, enable_all, set_priority
from app.api.tasks import cancel_task, submit_task
from app.core.task_repository import TaskRepository, format_log_entry
from app.core.timezone import now, set_default_timezone
from app.main import create_app
from app.models.schemas import (
    CancelRequest,
    PriorityLevel,
    PriorityRequest,
    TaskRecord,
    TaskRequest,
    TaskStatus,
)
from app.services.state_store import StateStore


class InMemoryTaskRepository(TaskRepository):
    def __init__(self):
        self._tasks: dict[str, TaskRecord] = {}
        self._index_tasks: set[str] = set()
        self._service_index: dict[str, set[str]] = {}
        self._service_users_index: dict[str, set[str]] = {}
        self._service_user_index: dict[tuple[str, str], set[str]] = {}
        self._sequence = 0

    async def next_task_id(self) -> str:
        self._sequence += 1
        return str(self._sequence)

    async def save(self, task: TaskRecord) -> None:
        self._tasks[task.task_id] = task
        self._index_tasks.add(task.task_id)
        self._service_index.setdefault(task.service, set()).add(task.task_id)
        self._service_users_index.setdefault(task.service, set()).add(task.user_id)
        self._service_user_index.setdefault((task.service, task.user_id), set()).add(task.task_id)

    async def get(self, task_id: str) -> TaskRecord | None:
        task = self._tasks.get(task_id)
        return task.model_copy() if task else None

    async def delete(self, task_id: str) -> None:
        task = self._tasks.pop(task_id, None)
        if not task:
            return
        self._index_tasks.discard(task_id)
        self._service_index.get(task.service, set()).discard(task_id)
        self._service_user_index.get((task.service, task.user_id), set()).discard(task_id)
        if (task.service, task.user_id) in self._service_user_index and not self._service_user_index[
            (task.service, task.user_id)
        ]:
            self._service_user_index.pop((task.service, task.user_id))
            self._service_users_index.get(task.service, set()).discard(task.user_id)

    async def set_status(self, task_id: str, status: TaskStatus, *, log_entry: str | None = None) -> TaskRecord | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.status = status
        task.updated_at = now()
        if log_entry:
            task.logs.append(format_log_entry(log_entry))
        await self.save(task)
        return task

    async def append_log(self, task_id: str, message: str) -> TaskRecord | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.logs.append(format_log_entry(message))
        task.updated_at = now()
        await self.save(task)
        return task

    async def update_result(
        self,
        task_id: str,
        *,
        pod_status: str | None = None,
        launcher_output: str | None = None,
    ) -> TaskRecord | None:
        task = await self.get(task_id)
        if not task:
            return None
        updated = task.result.model_copy()
        if pod_status is not None:
            updated.pod_status = pod_status
        if launcher_output is not None:
            updated.launcher_output = launcher_output
        task.result = updated
        task.updated_at = now()
        await self.save(task)
        return task

    async def list_by_ids(self, ids):
        return [task for task_id in ids if (task := await self.get(task_id))]

    async def list_all(self):
        return [await self.get(task_id) for task_id in self._index_tasks]

    async def list_by_service(self, service: str):
        return [await self.get(task_id) for task_id in self._service_index.get(service, set())]

    async def list_by_service_and_user(self, service: str, user_id: str):
        return [await self.get(task_id) for task_id in self._service_user_index.get((service, user_id), set())]

    async def list_users_by_service(self, service: str):
        return list(self._service_users_index.get(service, set()))


class InMemoryRedis:
    def __init__(self):
        self._data: dict[str, str] = {}

    async def connect(self):
        return None

    async def close(self):
        return None

    async def write_json(self, key: str, value):
        self._data[key] = json.dumps(value, default=str)

    async def read_json(self, key: str):
        raw = self._data.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def delete(self, key: str):
        self._data.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self._data


@pytest.fixture
def state_store(monkeypatch):
    monkeypatch.setenv("DMS_OPERATOR_TOKEN", "changeme")
    monkeypatch.setenv("DMS_TIMEZONE", "UTC")
    set_default_timezone("UTC")
    redis_client = InMemoryRedis()
    repository = InMemoryTaskRepository()
    store = StateStore(redis_client=redis_client, repository=repository)
    deps._state_store = store
    return store


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_submit_task_and_completion(state_store):
    payload = TaskRequest(task_id="10", service="sync", user_id="alice", parameters={"src": "/a", "dst": "/b"})
    preregistered = TaskRecord(
        task_id=payload.task_id,
        service=payload.service,
        user_id=payload.user_id,
        parameters=payload.parameters,
        status=TaskStatus.pending,
    )
    await deps.get_state_store().save_task(preregistered)

    response = await submit_task(payload, state_store=state_store)
    assert response["status"] == TaskStatus.dispatching

    await asyncio.sleep(0.2)
    state = await deps.get_state_store().load_task("10")
    assert state.status == TaskStatus.completed
    assert state.result.pod_status == "Succeeded"
    for log_entry in state.logs:
        timestamp, _, message = log_entry.partition(",")
        assert message
        parsed = datetime.fromisoformat(timestamp)
        assert parsed.tzinfo == timezone.utc


@pytest.mark.anyio("asyncio")
async def test_cancel_task(state_store):
    payload = TaskRequest(task_id="11", service="sync", user_id="bob", parameters={"src": "a", "dst": "b"})
    preregistered = TaskRecord(
        task_id=payload.task_id,
        service=payload.service,
        user_id=payload.user_id,
        parameters=payload.parameters,
        status=TaskStatus.pending,
    )
    await deps.get_state_store().save_task(preregistered)

    await submit_task(payload, state_store=state_store)
    await asyncio.sleep(0.05)
    cancel_request = CancelRequest(task_id=payload.task_id, service=payload.service, user_id=payload.user_id)
    response = await cancel_task(cancel_request, state_store=state_store)
    assert response["status"] == TaskStatus.cancel_requested
    state = await state_store.load_task("11")
    assert state.status == TaskStatus.cancel_requested


@pytest.mark.anyio("asyncio")
async def test_priority_update(state_store):
    payload = TaskRequest(task_id="12", service="sync", user_id="alice", parameters={"src": "x", "dst": "y"})
    preregistered = TaskRecord(
        task_id=payload.task_id,
        service=payload.service,
        user_id=payload.user_id,
        parameters=payload.parameters,
        status=TaskStatus.pending,
    )
    await deps.get_state_store().save_task(preregistered)
    await submit_task(payload, state_store=state_store)

    response = await set_priority(
        PriorityRequest(task_id=payload.task_id, priority=PriorityLevel.high), state_store=state_store
    )
    assert response["priority"] == PriorityLevel.high
    state = await state_store.load_task(payload.task_id)
    assert state.priority == PriorityLevel.high


@pytest.mark.anyio("asyncio")
async def test_blocking_behavior(state_store):
    block_response = await block_all(state_store=state_store)
    assert block_response["blocked"] is True

    payload = TaskRequest(task_id="blocked", service="sync", user_id="alice", parameters={})
    preregistered = TaskRecord(
        task_id=payload.task_id,
        service=payload.service,
        user_id=payload.user_id,
        parameters=payload.parameters,
        status=TaskStatus.pending,
    )
    await deps.get_state_store().save_task(preregistered)

    with pytest.raises(HTTPException):
        await submit_task(payload, state_store=state_store)

    enable_response = await enable_all(state_store=state_store)
    assert enable_response["blocked"] is False

    response = await submit_task(payload, state_store=state_store)
    assert response["status"] == TaskStatus.dispatching


@pytest.mark.anyio("asyncio")
async def test_help_endpoint(state_store):
    app = create_app(client=state_store.redis)
    help_route = next(route for route in app.routes if getattr(route, "path", "") == "/help")
    result = await help_route.endpoint()

    assert result["timezone"] == "UTC"
    assert "<iso-timestamp>,<message>" in result["log_format"]
