import logging
from ..models.schemas import PriorityLevel
from .state_store import StateStore

logger = logging.getLogger(__name__)


class AdminService:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    async def set_priority(self, task_id: str, priority: PriorityLevel) -> bool:
        state = await self.state_store.set_priority(task_id, priority, f"Priority set to {priority.value}")
        if state:
            logger.info("Priority for task %s set to %s", task_id, priority.value)
            return True
        return False

    async def block_all(self) -> None:
        await self.state_store.set_block_all(True)
        logger.warning("All frontend requests blocked")

    async def enable_all(self) -> None:
        await self.state_store.set_block_all(False)
        logger.info("Frontend requests enabled")

    async def block_user(self, user_id: str) -> None:
        await self.state_store.set_block_for_user(user_id, True)
        logger.warning("User %s blocked", user_id)

    async def enable_user(self, user_id: str) -> None:
        await self.state_store.set_block_for_user(user_id, False)
        logger.info("User %s enabled", user_id)
