from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime

import redis.asyncio as redis

from app.config import Settings
from app.models import Priority, QueueOverview, QueuePosition, StatsSnapshot, TaskRecord

TASK_KEY_PREFIX = "dms:task:"
QUEUE_KEY_PREFIX = "dms:queue:"
STATS_KEY = "dms:stats"
BLOCK_KEY = "dms:block:frontend"
TASK_INDEX_KEY = "dms:task:index"

PRIORITY_ORDER = [Priority.high, Priority.mid, Priority.low]


class RedisRepository:
    def __init__(self, writer: redis.Redis, reader: redis.Redis | None = None):
        self.writer = writer
        self.reader = reader or writer

    @classmethod
    def from_settings(cls, settings: Settings) -> RedisRepository:
        writer = redis.from_url(settings.redis_writer_url, encoding="utf-8", decode_responses=True)
        reader = redis.from_url(settings.redis_reader_url, encoding="utf-8", decode_responses=True)
        return cls(writer=writer, reader=reader)

    async def close(self) -> None:
        await self.writer.close()
        if self.reader is not self.writer:
            await self.reader.close()

    async def save_task(self, record: TaskRecord) -> None:
        key = f"{TASK_KEY_PREFIX}{record.task_id}"
        await self.writer.set(key, record.model_dump_json())
        await self.writer.sadd(TASK_INDEX_KEY, record.task_id)

    async def get_task(self, task_id: str) -> TaskRecord | None:
        key = f"{TASK_KEY_PREFIX}{task_id}"
        data = await self.reader.get(key)
        if not data:
            return None
        return TaskRecord.model_validate_json(data)

    async def delete_task(self, task_id: str) -> None:
        key = f"{TASK_KEY_PREFIX}{task_id}"
        await self.writer.delete(key)
        await self.writer.srem(TASK_INDEX_KEY, task_id)

    def _queue_key(self, priority: Priority) -> str:
        return f"{QUEUE_KEY_PREFIX}{priority.value}"

    async def enqueue(self, priority: Priority, task_id: str) -> None:
        await self.writer.rpush(self._queue_key(priority), task_id)

    async def dequeue(self, priority: Priority) -> str | None:
        return await self.writer.lpop(self._queue_key(priority))

    async def remove_from_queue(self, priority: Priority, task_id: str) -> int:
        return await self.writer.lrem(self._queue_key(priority), 0, task_id)

    async def queue_lengths(self) -> QueueOverview:
        high = await self.reader.llen(self._queue_key(Priority.high))
        mid = await self.reader.llen(self._queue_key(Priority.mid))
        low = await self.reader.llen(self._queue_key(Priority.low))
        return QueueOverview(total=high + mid + low, high=high, mid=mid, low=low)

    async def queue_snapshot(self) -> dict[Priority, list[str]]:
        result: dict[Priority, list[str]] = {}
        for priority in PRIORITY_ORDER:
            key = self._queue_key(priority)
            result[priority] = await self.reader.lrange(key, 0, -1)
        return result

    async def pop_next_task(self) -> str | None:
        for priority in PRIORITY_ORDER:
            task_id = await self.dequeue(priority)
            if task_id:
                return task_id
        return None

    async def increment_stat(self, field: str, amount: int = 1) -> None:
        await self.writer.hincrby(STATS_KEY, field, amount)
        await self.writer.hset(STATS_KEY, "last_updated", datetime.utcnow().isoformat())

    async def read_stats(self, overview: QueueOverview) -> StatsSnapshot:
        raw = await self.reader.hgetall(STATS_KEY)
        queued = int(raw.get("queued", 0))
        dispatched = int(raw.get("dispatched", 0))
        canceled = int(raw.get("canceled", 0))
        last_updated_raw = raw.get("last_updated", datetime.utcnow().isoformat())
        last_updated = datetime.fromisoformat(last_updated_raw)
        return StatsSnapshot(
            queued=queued,
            dispatched=dispatched,
            canceled=canceled,
            last_updated=last_updated,
            queue_overview=overview,
        )

    async def set_blocked(self, blocked: bool, reason: str | None = None) -> None:
        value = json.dumps(
            {"blocked": blocked, "reason": reason, "updated_at": datetime.utcnow().isoformat()}
        )
        await self.writer.set(BLOCK_KEY, value)

    async def get_blocked(self) -> dict:
        value = await self.reader.get(BLOCK_KEY)
        if not value:
            return {"blocked": False, "reason": None}
        return json.loads(value)

    async def find_position(self, task_id: str) -> QueuePosition | None:
        snapshot = await self.queue_snapshot()
        global_index = 0
        for priority in PRIORITY_ORDER:
            queue = snapshot[priority]
            if task_id in queue:
                priority_index = queue.index(task_id)
                return QueuePosition(
                    priority=priority,
                    position_in_priority=priority_index + 1,
                    global_position=global_index + priority_index + 1,
                )
            global_index += len(queue)
        return None

    async def iter_tasks(self) -> Iterable[str]:
        return await self.reader.smembers(TASK_INDEX_KEY)
