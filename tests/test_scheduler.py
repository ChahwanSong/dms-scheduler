from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import FastAPI
from httpx import AsyncClient

from app import dependencies
from app.config import Settings, get_settings
from app.main import create_app
from app.redis_client import RedisRepository


class MiniRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}
        self.hashes: dict[str, dict[str, Any]] = {}

    async def close(self) -> None:  # pragma: no cover - compatibility shim
        return None

    # String operations
    async def set(self, key: str, value: str) -> None:
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)

    # Set operations
    async def sadd(self, key: str, member: str) -> None:
        self.sets.setdefault(key, set()).add(member)

    async def srem(self, key: str, member: str) -> None:
        if key in self.sets:
            self.sets[key].discard(member)

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))

    # List operations
    async def rpush(self, key: str, member: str) -> None:
        self.lists.setdefault(key, []).append(member)

    async def lpop(self, key: str) -> str | None:
        values = self.lists.get(key, [])
        if values:
            return values.pop(0)
        return None

    async def llen(self, key: str) -> int:
        return len(self.lists.get(key, []))

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        values = self.lists.get(key, [])
        slice_end = None if end == -1 else end + 1
        return values[start:slice_end]

    async def lrem(self, key: str, _count: int, member: str) -> int:
        values = self.lists.get(key, [])
        removed = 0
        filtered: list[str] = []
        for value in values:
            if value == member:
                removed += 1
            else:
                filtered.append(value)
        self.lists[key] = filtered
        return removed

    # Hash operations
    async def hgetall(self, key: str) -> dict[str, str]:
        return {k: str(v) for k, v in self.hashes.get(key, {}).items()}

    async def hincrby(self, key: str, field: str, amount: int) -> int:
        hash_obj = self.hashes.setdefault(key, {})
        hash_obj[field] = int(hash_obj.get(field, 0)) + amount
        return hash_obj[field]

    async def hset(self, key: str, field: str, value: Any) -> None:
        self.hashes.setdefault(key, {})[field] = value


@pytest.fixture
async def test_app() -> AsyncIterator[FastAPI]:
    dependencies.get_repository.cache_clear()
    fake_redis = MiniRedis()
    repository = RedisRepository(writer=fake_redis, reader=fake_redis)

    app = create_app()
    app.dependency_overrides[dependencies.get_repository] = lambda: repository
    app.dependency_overrides[get_settings] = lambda: Settings(
        operator_token="secret-token",
        redis_writer_url="redis://fake",
        redis_reader_url="redis://fake",
    )

    yield app

    await repository.close()


@pytest.mark.asyncio
async def test_priority_dispatch_and_positions(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        await client.post(
            "/tasks",
            json={
                "task_id": "1",
                "service": "sync",
                "user_id": "alice",
                "parameters": {"src": "/a", "dst": "/b"},
                "priority": "low",
            },
        )
        await client.post(
            "/tasks",
            json={
                "task_id": "2",
                "service": "sync",
                "user_id": "bob",
                "parameters": {"src": "/c", "dst": "/d"},
                "priority": "high",
            },
            headers={"X-DMS-Operator-Token": "secret-token"},
        )

        summary_low = await client.get("/tasks/1")
        summary_high = await client.get("/tasks/2")

        assert summary_high.json()["queue_position"]["global_position"] == 1
        assert summary_low.json()["queue_position"]["global_position"] == 2

        dispatch_high = await client.post("/submitter/next")
        assert dispatch_high.status_code == 200
        assert dispatch_high.json()["task_id"] == "2"

        dispatch_low = await client.post("/submitter/next")
        assert dispatch_low.json()["task_id"] == "1"


@pytest.mark.asyncio
async def test_priority_change_by_admin(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        await client.post(
            "/tasks",
            json={
                "task_id": "3",
                "service": "sync",
                "user_id": "alice",
                "parameters": {},
                "priority": "low",
            },
        )

        promote = await client.post(
            "/admin/priority",
            headers={"X-DMS-Operator-Token": "secret-token"},
            json={"task_id": "3", "priority": "high"},
        )
        assert promote.status_code == 200
        assert promote.json()["priority"] == "high"


@pytest.mark.asyncio
async def test_blocking_prevents_new_tasks(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        toggle = await client.post(
            "/admin/block",
            headers={"X-DMS-Operator-Token": "secret-token"},
            json={"blocked": True, "reason": "maintenance"},
        )
        assert toggle.status_code == 200
        assert toggle.json()["blocked"] is True

        create = await client.post(
            "/tasks",
            json={
                "task_id": "4",
                "service": "sync",
                "user_id": "block",
                "parameters": {},
            },
        )
        assert create.status_code == 503


@pytest.mark.asyncio
async def test_block_resume_allows_new_tasks(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        await client.post(
            "/admin/block",
            headers={"X-DMS-Operator-Token": "secret-token"},
            json={"blocked": True, "reason": "maintenance"},
        )

        resume = await client.post(
            "/admin/resume",
            headers={"X-DMS-Operator-Token": "secret-token"},
            json={"reason": "maintenance complete"},
        )
        assert resume.status_code == 200
        assert resume.json()["blocked"] is False

        create = await client.post(
            "/tasks",
            json={
                "task_id": "resume-1",
                "service": "sync",
                "user_id": "block",
                "parameters": {},
            },
        )
        assert create.status_code == 201


@pytest.mark.asyncio
async def test_high_priority_requires_operator_token(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        response = await client.post(
            "/tasks",
            json={
                "task_id": "secured",
                "service": "sync",
                "user_id": "alice",
                "parameters": {},
                "priority": "high",
            },
        )

        assert response.status_code == 403


@pytest.mark.asyncio
async def test_cancel_removes_from_queue(test_app: FastAPI):
    async with AsyncClient(app=test_app, base_url="http://test") as client:
        await client.post(
            "/tasks",
            json={
                "task_id": "5",
                "service": "sync",
                "user_id": "carol",
                "parameters": {},
            },
        )

        cancel = await client.post(
            "/tasks/cancel",
            json={"task_id": "5", "service": "sync", "user_id": "carol"},
        )
        assert cancel.status_code == 200
        assert cancel.json()["status"] == "canceled"

        summary = await client.get("/tasks/5")
        assert summary.json()["queue_position"] is None
