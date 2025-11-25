import asyncio
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.api import deps
from app.main import create_app
from app.core.redis import RedisClient
from app.core.task_repository import TaskRepository, format_log_entry
from app.core.timezone import now, set_default_timezone
from app.models.schemas import TaskRecord, TaskStatus
from app.services.state_store import StateStore


class InMemoryRedis(RedisClient):
    def __init__(self):
        super().__init__()
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
        if (task.service, task.user_id) in self._service_user_index and not self._service_user_index[(task.service, task.user_id)]:
            self._service_user_index.pop((task.service, task.user_id))
            self._service_users_index.get(task.service, set()).discard(task.user_id)

    async def set_status(
        self, task_id: str, status: TaskStatus, *, log_entry: str | None = None
    ) -> TaskRecord | None:
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
        return [
            await self.get(task_id)
            for task_id in self._service_user_index.get((service, user_id), set())
        ]

    async def list_users_by_service(self, service: str):
        return list(self._service_users_index.get(service, set()))


@pytest.fixture
def state_store(monkeypatch):
    monkeypatch.setenv("DMS_OPERATOR_TOKEN", "changeme")
    monkeypatch.setenv("DMS_TIMEZONE", "UTC")
    set_default_timezone("UTC")
    redis_client = InMemoryRedis()
    repository = InMemoryTaskRepository()
    store = StateStore(redis_client=redis_client, repository=repository)
    return store


@pytest.fixture
def client(state_store):
    app = create_app(client=state_store.redis)
    deps._state_store = state_store
    with TestClient(app) as test_client:
        yield test_client


def test_submit_task_and_completion(client):
    payload = {"task_id": "10", "service": "sync", "user_id": "alice", "parameters": {"src": "/a", "dst": "/b"}}
    preregistered = TaskRecord(
        task_id=payload["task_id"],
        service=payload["service"],
        user_id=payload["user_id"],
        parameters=payload["parameters"],
        status=TaskStatus.pending,
    )
    asyncio.run(deps.get_state_store().save_task(preregistered))
    response = client.post("/tasks/task", json=payload)
    assert response.status_code == 202
    assert response.json()["status"] == TaskStatus.dispatching

    asyncio.run(asyncio.sleep(0.2))
    state = asyncio.run(deps.get_state_store().load_task("10"))
    assert state.status == TaskStatus.completed
    assert state.result.pod_status == "Succeeded"
    for log_entry in state.logs:
        timestamp, _, message = log_entry.partition(",")
        assert message
        parsed = datetime.fromisoformat(timestamp)
        assert parsed.tzinfo == timezone.utc


def test_cancel_task(client, state_store):
    payload = {"task_id": "11", "service": "sync", "user_id": "bob", "parameters": {"src": "a", "dst": "b"}}
    preregistered = TaskRecord(
        task_id=payload["task_id"],
        service=payload["service"],
        user_id=payload["user_id"],
        parameters=payload["parameters"],
        status=TaskStatus.pending,
    )
    asyncio.run(deps.get_state_store().save_task(preregistered))
    client.post("/tasks/task", json=payload)
    asyncio.run(asyncio.sleep(0.05))
    response = client.post("/tasks/cancel", json={"task_id": "11", "service": "sync", "user_id": "bob"})
    assert response.status_code == 202
    state = asyncio.run(state_store.load_task("11"))
    assert state.status == TaskStatus.cancel_requested


def test_blocking_behavior(client):
    operator_headers = {"X-Operator-Token": "changeme"}
    block_response = client.post("/admin/block", headers=operator_headers)
    assert block_response.status_code == 200

    payload = {"task_id": "blocked", "service": "sync", "user_id": "alice", "parameters": {}}
    response = client.post("/tasks/task", json=payload)
    assert response.status_code == 403

    client.post("/admin/enable", headers=operator_headers)
    response = client.post("/tasks/task", json=payload)
    assert response.status_code == 202


def test_priority_update(client, state_store):
    payload = {"task_id": "12", "service": "sync", "user_id": "alice", "parameters": {}}
    preregistered = TaskRecord(
        task_id=payload["task_id"],
        service=payload["service"],
        user_id=payload["user_id"],
        parameters=payload["parameters"],
        status=TaskStatus.pending,
    )
    asyncio.run(deps.get_state_store().save_task(preregistered))
    client.post("/tasks/task", json=payload)
    headers = {"X-Operator-Token": "changeme"}
    response = client.post("/admin/priority", json={"task_id": "12", "priority": "high"}, headers=headers)
    assert response.status_code == 202
    state = asyncio.run(state_store.load_task("12"))
    assert state.priority == state.priority.high


def test_help_endpoint(client):
    response = client.get("/help")
    assert response.status_code == 200
    body = response.json()
    assert body["timezone"] == "UTC"
    assert "<iso-timestamp>,<message>" in body["log_format"]
