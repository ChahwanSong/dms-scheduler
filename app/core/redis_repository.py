"""Redis connection helpers for shared task repository usage."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from redis.asyncio import Redis

from .settings import get_settings
from .task_repository import RedisTaskRepository
from .timezone import DEFAULT_TIMEZONE_NAME, _coerce_timezone, set_default_timezone


@dataclass(slots=True)
class RedisRepositorySettings:
    """Configuration required to build a Redis-backed repository."""

    write_url: str
    read_url: str
    ttl_seconds: int
    decode_responses: bool = True
    timezone_name: str = DEFAULT_TIMEZONE_NAME

    @classmethod
    def from_env(
        cls,
        *,
        ttl_seconds: Optional[int] = None,
        write_env: str = "DMS_REDIS_WRITE_URL",
        read_env: str = "DMS_REDIS_READ_URL",
        timezone_env: str = "DMS_TIMEZONE",
    ) -> "RedisRepositorySettings":
        """Load configuration from environment variables.

        Falls back to the write URL when a dedicated read URL is not supplied.
        """

        settings_model = get_settings()
        write_url = os.getenv(write_env, settings_model.redis_write_url)
        read_url = os.getenv(read_env, write_url)
        if ttl_seconds is None:
            ttl_env = os.getenv("DMS_REDIS_TASK_TTL_SECONDS")
            ttl_seconds = int(ttl_env) if ttl_env else 90 * 24 * 60 * 60
        timezone_name = os.getenv(timezone_env, DEFAULT_TIMEZONE_NAME)
        _coerce_timezone(timezone_name)
        return cls(
            write_url=write_url,
            read_url=read_url,
            ttl_seconds=int(ttl_seconds),
            timezone_name=timezone_name,
        )


class RedisRepositoryProvider:
    """Manage Redis clients and expose a ready-to-use repository instance."""

    def __init__(self, settings: RedisRepositorySettings) -> None:
        self._settings = settings
        self._reader: Optional[Redis] = None
        self._writer: Optional[Redis] = None
        self._repository: Optional[RedisTaskRepository] = None

    @property
    def reader(self) -> Optional[Redis]:
        return self._reader

    @property
    def writer(self) -> Optional[Redis]:
        return self._writer

    async def get_repository(self) -> RedisTaskRepository:
        """Create (or return) the Redis-backed repository."""

        if self._repository is None:
            set_default_timezone(self._settings.timezone_name)
            writer = Redis.from_url(
                self._settings.write_url, decode_responses=self._settings.decode_responses
            )
            reader = Redis.from_url(
                self._settings.read_url, decode_responses=self._settings.decode_responses
            )
            try:
                await writer.ping()
                await reader.ping()
            except Exception:
                await writer.aclose()
                await reader.aclose()
                raise
            self._writer = writer
            self._reader = reader
            self._repository = RedisTaskRepository(
                reader=reader,
                writer=writer,
                ttl_seconds=self._settings.ttl_seconds,
                tzinfo=_coerce_timezone(self._settings.timezone_name),
            )
        return self._repository

    async def close(self) -> None:
        """Close the Redis clients created by the provider."""

        if self._reader:
            await self._reader.aclose()
        if self._writer:
            await self._writer.aclose()
        self._reader = None
        self._writer = None
        self._repository = None
