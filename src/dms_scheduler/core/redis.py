import json
from typing import Any

from redis.asyncio import Redis

from .settings import get_settings


class RedisClient:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.write_client: Redis | None = None
        self.read_client: Redis | None = None

    async def connect(self) -> None:
        if self.write_client is None:
            self.write_client = Redis(
                host=self.settings.redis_write_host,
                port=self.settings.redis_write_port,
                decode_responses=True,
            )
        if self.read_client is None:
            self.read_client = Redis(
                host=self.settings.redis_read_host,
                port=self.settings.redis_read_port,
                decode_responses=True,
            )

    async def close(self) -> None:
        if self.write_client:
            await self.write_client.aclose()
        if self.read_client:
            await self.read_client.aclose()

    async def write_json(self, key: str, value: Any) -> None:
        if self.write_client is None:
            await self.connect()
        assert self.write_client
        await self.write_client.set(key, json.dumps(value, default=str))

    async def read_json(self, key: str) -> Any:
        if self.read_client is None:
            await self.connect()
        assert self.read_client
        raw = await self.read_client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def delete(self, key: str) -> None:
        if self.write_client is None:
            await self.connect()
        assert self.write_client
        await self.write_client.delete(key)

    async def exists(self, key: str) -> bool:
        if self.read_client is None:
            await self.connect()
        assert self.read_client
        return bool(await self.read_client.exists(key))
