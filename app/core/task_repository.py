"""Task repository implementations that can be reused across projects."""

from __future__ import annotations

import abc
import inspect
from datetime import tzinfo
from typing import Awaitable, Iterable, List, TypeVar, cast

from redis.asyncio import Redis

from ..models.schemas import TaskRecord, TaskResult, TaskStatus
from .timezone import get_default_timezone, now


def format_log_entry(message: str, *, tzinfo: tzinfo | None = None) -> str:
    """Return a timestamped log entry string.

    Entries are encoded as ``"<iso-timestamp>,<message>"`` so consumers can
    parse the timestamp without losing the original text payload.
    """

    timestamp = now(tzinfo).isoformat()
    return f"{timestamp},{message}"


class TaskRepository(abc.ABC):
    """Abstract task repository interface shared by services."""

    @abc.abstractmethod
    async def next_task_id(self) -> str: ...

    @abc.abstractmethod
    async def save(self, task: TaskRecord) -> None: ...

    @abc.abstractmethod
    async def get(self, task_id: str) -> TaskRecord | None: ...

    @abc.abstractmethod
    async def delete(self, task_id: str) -> None: ...

    @abc.abstractmethod
    async def set_status(
        self, task_id: str, status: TaskStatus, *, log_entry: str | None = None
    ) -> TaskRecord | None: ...

    @abc.abstractmethod
    async def append_log(self, task_id: str, message: str) -> TaskRecord | None: ...

    @abc.abstractmethod
    async def update_result(
        self,
        task_id: str,
        *,
        pod_status: str | None = None,
        launcher_output: str | None = None,
    ) -> TaskRecord | None: ...

    @abc.abstractmethod
    async def list_by_ids(self, ids: Iterable[str]) -> List[TaskRecord]: ...

    @abc.abstractmethod
    async def list_all(self) -> List[TaskRecord]: ...

    @abc.abstractmethod
    async def list_by_service(self, service: str) -> List[TaskRecord]: ...

    @abc.abstractmethod
    async def list_by_service_and_user(self, service: str, user_id: str) -> List[TaskRecord]: ...

    @abc.abstractmethod
    async def list_users_by_service(self, service: str) -> List[str]: ...


_T = TypeVar("_T")


class RedisTaskRepository(TaskRepository):
    """Task repository backed by Redis key value store."""

    _METADATA_TTL_GRACE_SECONDS = 60

    def __init__(self, reader: Redis, writer: Redis, *, ttl_seconds: int, tzinfo=None) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive integer")
        self._reader = reader
        self._writer = writer
        self._ttl_seconds = int(ttl_seconds)
        self._timezone = tzinfo or get_default_timezone()
        self._metadata_ttl_seconds = self._ttl_seconds + self._METADATA_TTL_GRACE_SECONDS

    async def _execute(self, command: Awaitable[_T] | _T) -> _T:
        if inspect.isawaitable(command):
            return await cast(Awaitable[_T], command)
        return cast(_T, command)

    async def next_task_id(self) -> str:
        next_id = await self._execute(self._writer.incr("task:id:sequence"))
        return str(next_id)

    async def save(self, task: TaskRecord) -> None:
        await self._execute(
            self._writer.set(
                self._task_key(task.task_id),
                task.model_dump_json(),
                ex=self._ttl_seconds,
            )
        )
        await self._save_task_metadata(task)
        await self._execute(self._writer.sadd("index:tasks", task.task_id))
        await self._ensure_ttl("index:tasks")
        service_index = self._service_index(task.service)
        await self._execute(self._writer.sadd(service_index, task.task_id))
        await self._ensure_ttl(service_index)
        service_users_index = self._service_users_index(task.service)
        await self._execute(self._writer.sadd(service_users_index, task.user_id))
        await self._ensure_ttl(service_users_index)
        service_user_index = self._service_user_index(task.service, task.user_id)
        await self._execute(self._writer.sadd(service_user_index, task.task_id))
        await self._ensure_ttl(service_user_index)

    async def get(self, task_id: str) -> TaskRecord | None:
        raw = await self._execute(self._reader.get(self._task_key(task_id)))
        if not raw:
            return None
        return TaskRecord.model_validate_json(raw)

    async def delete(self, task_id: str) -> None:
        task = await self.get(task_id)
        if not task:
            return
        await self._execute(self._writer.delete(self._task_key(task_id)))
        await self._cleanup_indices(task_id, service=task.service, user_id=task.user_id)
        await self._execute(self._writer.delete(self._task_metadata_key(task_id)))

    async def set_status(
        self, task_id: str, status: TaskStatus, *, log_entry: str | None = None
    ) -> TaskRecord | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.status = status
        task.updated_at = now(self._timezone)
        if log_entry:
            task.logs.append(format_log_entry(log_entry, tzinfo=self._timezone))
        await self.save(task)
        return task

    async def append_log(self, task_id: str, message: str) -> TaskRecord | None:
        task = await self.get(task_id)
        if not task:
            return None
        task.logs.append(format_log_entry(message, tzinfo=self._timezone))
        task.updated_at = now(self._timezone)
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

        result = task.result or TaskResult()
        updated = result.model_copy()
        changed = False

        if pod_status is not None:
            updated.pod_status = pod_status
            changed = True
        if launcher_output is not None:
            updated.launcher_output = launcher_output
            changed = True

        if not changed:
            return task

        task.result = updated
        task.updated_at = now(self._timezone)
        await self.save(task)
        return task

    async def list_by_ids(self, ids: Iterable[str]) -> List[TaskRecord]:
        ids_list = list(ids)
        if not ids_list:
            return []
        raw_values = await self._execute(
            self._reader.mget([self._task_key(task_id) for task_id in ids_list])
        )
        return [TaskRecord.model_validate_json(raw) for raw in raw_values if raw]

    async def list_all(self) -> List[TaskRecord]:
        ids = await self._execute(self._reader.smembers("index:tasks"))
        return await self.list_by_ids(ids)

    async def list_by_service(self, service: str) -> List[TaskRecord]:
        ids = await self._execute(self._reader.smembers(self._service_index(service)))
        return await self.list_by_ids(ids)

    async def list_by_service_and_user(self, service: str, user_id: str) -> List[TaskRecord]:
        ids = await self._execute(
            self._reader.smembers(self._service_user_index(service, user_id))
        )
        return await self.list_by_ids(ids)

    async def list_users_by_service(self, service: str) -> List[str]:
        raw_users = await self._execute(self._reader.smembers(self._service_users_index(service)))
        return [user.decode() if isinstance(user, bytes) else str(user) for user in raw_users]

    async def handle_task_expired(self, task_id: str) -> None:
        """Remove index entries for an expired task if metadata is available."""

        metadata = await self._execute(self._reader.hgetall(self._task_metadata_key(task_id)))
        service = metadata.get("service") if metadata else None
        user_id = metadata.get("user_id") if metadata else None

        if not service or not user_id:
            return

        await self._cleanup_indices(task_id, service=service, user_id=user_id)
        await self._execute(self._writer.delete(self._task_metadata_key(task_id)))

    @staticmethod
    def _task_key(task_id: str) -> str:
        return f"task:{task_id}"

    @staticmethod
    def _service_index(service: str) -> str:
        return f"index:service:{service}"

    @staticmethod
    def _service_user_index(service: str, user_id: str) -> str:
        return f"index:service:{service}:user:{user_id}"

    @staticmethod
    def _service_users_index(service: str) -> str:
        return f"index:service:{service}:users"

    @staticmethod
    def _task_metadata_key(task_id: str) -> str:
        return f"task:{task_id}:metadata"

    async def _ensure_ttl(self, key: str) -> None:
        await self._execute(self._writer.expire(key, self._ttl_seconds))

    async def _cleanup_user_index(self, service: str, user_id: str) -> None:
        service_user_index = self._service_user_index(service, user_id)
        remaining = await self._execute(self._reader.scard(service_user_index))
        if remaining:
            await self._ensure_ttl(service_user_index)
            return
        await self._execute(self._writer.srem(self._service_users_index(service), user_id))
        await self._ensure_ttl(self._service_users_index(service))

    async def _save_task_metadata(self, task: TaskRecord) -> None:
        metadata_key = self._task_metadata_key(task.task_id)
        await self._execute(
            self._writer.hset(
                metadata_key,
                mapping={"service": task.service, "user_id": task.user_id},
            )
        )
        await self._execute(self._writer.expire(metadata_key, self._metadata_ttl_seconds))

    async def _cleanup_indices(self, task_id: str, *, service: str, user_id: str) -> None:
        await self._execute(self._writer.srem("index:tasks", task_id))
        await self._execute(self._writer.srem(self._service_index(service), task_id))
        await self._execute(
            self._writer.srem(self._service_user_index(service, user_id), task_id)
        )
        await self._cleanup_user_index(service, user_id)
