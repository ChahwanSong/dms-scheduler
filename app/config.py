from __future__ import annotations

import logging
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    app_name: str = Field(default="dms-scheduler")
    redis_writer_url: str = Field(
        default="redis://haproxy-redis.redis.svc.cluster.local:6379/0"
    )
    redis_reader_url: str = Field(
        default="redis://haproxy-redis.redis.svc.cluster.local:6380/0"
    )
    operator_token: str = Field(default="changeme")
    environment: str = Field(default="local")
    log_level: str = Field(default="INFO")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    return settings


def require_operator_token(provided: str | None, settings: Settings) -> None:
    expected = settings.operator_token
    if not expected:
        raise PermissionError("Operator token is not configured.")
    if provided != expected:
        raise PermissionError("Invalid operator token.")
