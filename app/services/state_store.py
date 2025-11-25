import asyncio

from ..core.redis import RedisClient
from ..core.redis_repository import RedisRepositoryProvider, RedisRepositorySettings
from ..core.task_repository import TaskRepository
from ..core.task_repository import format_log_entry
from ..core.timezone import now
from ..models.schemas import BlockStatus, PriorityLevel, TaskRecord, TaskResult, TaskStatus

TASK_KEY_PREFIX = "task:"
BLOCK_ALL_KEY = "blocks:all"
BLOCK_USER_KEY_PREFIX = "blocks:user:"


class StateStore:
    def __init__(
        self,
        redis_client: RedisClient | None = None,
        repository: TaskRepository | None = None,
        repository_provider: RedisRepositoryProvider | None = None,
    ):
        self.redis = redis_client or RedisClient()
        self._repository_override = repository
        self._repository_provider = repository_provider
        self._lock = asyncio.Lock()

    async def _get_repository(self) -> TaskRepository:
        if self._repository_override:
            return self._repository_override
        if self._repository_provider is None:
            settings = RedisRepositorySettings.from_env()
            self._repository_provider = RedisRepositoryProvider(settings)
        return await self._repository_provider.get_repository()

    async def save_task(self, state: TaskRecord) -> None:
        repository = await self._get_repository()
        await repository.save(state)

    async def load_task(self, task_id: str) -> TaskRecord | None:
        repository = await self._get_repository()
        return await repository.get(task_id)

    async def get_task(self, task_id: str) -> TaskRecord | None:
        repository = await self._get_repository()
        return await repository.get(task_id)

    async def append_log(self, task_id: str, message: str) -> TaskRecord | None:
        repository = await self._get_repository()
        async with self._lock:
            return await repository.append_log(task_id, message)

    async def set_status(
        self, task_id: str, status: TaskStatus, message: str | None = None
    ) -> TaskRecord | None:
        repository = await self._get_repository()
        async with self._lock:
            return await repository.set_status(task_id, status, log_entry=message)

    async def set_result(
        self, task_id: str, result: TaskResult, message: str | None = None
    ) -> TaskRecord | None:
        repository = await self._get_repository()
        async with self._lock:
            updated = await repository.update_result(
                task_id,
                pod_status=result.pod_status,
                launcher_output=result.launcher_output,
            )
            if updated and message:
                updated.logs.append(format_log_entry(message))
                updated.updated_at = now()
                await repository.save(updated)
            return updated

    async def set_priority(
        self, task_id: str, priority: PriorityLevel, message: str | None = None
    ) -> TaskRecord | None:
        repository = await self._get_repository()
        async with self._lock:
            task = await repository.get(task_id)
            if not task:
                return None
            task.priority = priority
            task.updated_at = now()
            if message:
                task.logs.append(format_log_entry(message))
            await repository.save(task)
            return task

    async def set_block_all(self, blocked: bool) -> BlockStatus:
        status = await self.get_block_status()
        status.blocked = blocked
        await self.redis.write_json(BLOCK_ALL_KEY, status.model_dump())
        return status

    async def set_block_for_user(self, user_id: str, blocked: bool) -> BlockStatus:
        async with self._lock:
            status = await self.get_block_status()
            if blocked and user_id not in status.blocked_users:
                status.blocked_users.append(user_id)
            if not blocked and user_id in status.blocked_users:
                status.blocked_users.remove(user_id)
            await self.redis.write_json(BLOCK_ALL_KEY, status.model_dump())
            await self.redis.write_json(f"{BLOCK_USER_KEY_PREFIX}{user_id}", {"blocked": blocked})
            return status

    async def get_block_status(self) -> BlockStatus:
        data = await self.redis.read_json(BLOCK_ALL_KEY) or {}
        return BlockStatus(**data)

    async def is_blocked(self, user_id: str) -> bool:
        status = await self.get_block_status()
        if status.blocked:
            return True
        user_block = await self.redis.read_json(f"{BLOCK_USER_KEY_PREFIX}{user_id}")
        return bool(user_block and user_block.get("blocked"))
