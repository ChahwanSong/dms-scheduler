import asyncio
import json
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.api import deps
from app.main import create_app
from app.services.state_store import StateStore
from app.core.redis import RedisClient


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


@pytest.fixture
def state_store(monkeypatch):
    monkeypatch.setenv("DMS_OPERATOR_TOKEN", "changeme")
    monkeypatch.setenv("DMS_TIMEZONE", "UTC")
    redis_client = InMemoryRedis()
    store = StateStore(redis_client)
    return store


@pytest.fixture
def client(state_store):
    app = create_app(client=state_store.redis)
    deps._state_store = state_store
    with TestClient(app) as test_client:
        yield test_client


def test_submit_task_and_completion(client):
    payload = {"task_id": "10", "service": "sync", "user_id": "alice", "parameters": {"src": "/a", "dst": "/b"}}
    response = client.post("/tasks/task", json=payload)
    assert response.status_code == 202
    assert response.json()["status"] == "accepted"

    asyncio.run(asyncio.sleep(0.2))
    state = asyncio.run(deps.get_state_store().load_task("10"))
    assert state.status == state.status.completed
    assert state.result.pod_status == "Succeeded"
    for log_entry in state.logs:
        timestamp, _, message = log_entry.partition(",")
        assert message
        parsed = datetime.fromisoformat(timestamp)
        assert parsed.tzinfo == timezone.utc


def test_cancel_task(client, state_store):
    payload = {"task_id": "11", "service": "sync", "user_id": "bob", "parameters": {"src": "a", "dst": "b"}}
    client.post("/tasks/task", json=payload)
    asyncio.run(asyncio.sleep(0.05))
    response = client.post("/tasks/cancel", json={"task_id": "11", "service": "sync", "user_id": "bob"})
    assert response.status_code == 202
    state = asyncio.run(state_store.load_task("11"))
    assert state.status == state.status.cancelled


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
