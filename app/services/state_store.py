import asyncio

from ..core.redis import RedisClient
from ..core.timezone import format_log_message, now_in_configured_tz
from ..models.schemas import BlockStatus, PriorityLevel, TaskResult, TaskState, TaskStatus

TASK_KEY_PREFIX = "task:"
BLOCK_ALL_KEY = "blocks:all"
BLOCK_USER_KEY_PREFIX = "blocks:user:"


class StateStore:
    def __init__(self, redis_client: RedisClient | None = None):
        self.redis = redis_client or RedisClient()
        self._lock = asyncio.Lock()

    def _task_key(self, task_id: str) -> str:
        return f"{TASK_KEY_PREFIX}{task_id}"

    async def save_task(self, state: TaskState) -> None:
        await self.redis.write_json(self._task_key(state.task_id), state.model_dump())

    async def load_task(self, task_id: str) -> TaskState | None:
        data = await self.redis.read_json(self._task_key(task_id))
        if not data:
            return None
        return TaskState(**data)

    async def append_log(self, task_id: str, message: str) -> TaskState | None:
        async with self._lock:
            state = await self.load_task(task_id)
            if not state:
                return None
            state.logs.append(format_log_message(message))
            state.updated_at = now_in_configured_tz()
            await self.save_task(state)
            return state

    async def set_status(self, task_id: str, status: TaskStatus, message: str | None = None) -> TaskState | None:
        async with self._lock:
            state = await self.load_task(task_id)
            if not state:
                return None
            state.status = status
            state.updated_at = now_in_configured_tz()
            if message:
                state.logs.append(format_log_message(message))
            await self.save_task(state)
            return state

    async def set_result(self, task_id: str, result: TaskResult, message: str | None = None) -> TaskState | None:
        async with self._lock:
            state = await self.load_task(task_id)
            if not state:
                return None
            state.result = result
            state.updated_at = now_in_configured_tz()
            if message:
                state.logs.append(format_log_message(message))
            await self.save_task(state)
            return state

    async def set_priority(self, task_id: str, priority: PriorityLevel, message: str | None = None) -> TaskState | None:
        async with self._lock:
            state = await self.load_task(task_id)
            if not state:
                return None
            state.priority = priority
            state.updated_at = now_in_configured_tz()
            if message:
                state.logs.append(format_log_message(message))
            await self.save_task(state)
            return state

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
