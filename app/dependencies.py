from __future__ import annotations

from functools import lru_cache

from fastapi import Depends

from app.config import Settings, get_settings
from app.redis_client import RedisRepository
from app.services.tasks import TaskService


@lru_cache
def get_repository(settings: Settings = Depends(get_settings)) -> RedisRepository:
    return RedisRepository.from_settings(settings)


def get_task_service(repository: RedisRepository = Depends(get_repository)) -> TaskService:
    return TaskService(repository)
