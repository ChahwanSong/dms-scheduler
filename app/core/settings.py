from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    redis_write_url: str = Field(
        "redis://haproxy-redis.dms-redis.svc.cluster.local:6379/0",
        validation_alias=AliasChoices("DMS_REDIS_WRITE_URL", "redis_write_url"),
    )
    redis_read_url: str = Field(
        "redis://haproxy-redis.dms-redis.svc.cluster.local:6380/0",
        validation_alias=AliasChoices("DMS_REDIS_READ_URL", "redis_read_url"),
    )
    service_name: str = Field(
        "dms-scheduler",
        validation_alias=AliasChoices("DMS_SERVICE_NAME", "service_name"),
    )
    log_level: str = Field(
        "INFO", validation_alias=AliasChoices("DMS_LOG_LEVEL", "log_level")
    )
    timezone: str = Field(
        "Asia/Seoul", validation_alias=AliasChoices("DMS_TIMEZONE", "timezone")
    )


def get_settings() -> Settings:
    return Settings()
