from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)

    redis_write_host: str = Field(
        "haproxy-redis.redis.svc.cluster.local",
        validation_alias=AliasChoices("DMS_REDIS_WRITE_HOST", "redis_write_host"),
    )
    redis_write_port: int = Field(
        6379, validation_alias=AliasChoices("DMS_REDIS_WRITE_PORT", "redis_write_port")
    )
    redis_read_host: str = Field(
        "haproxy-redis.redis.svc.cluster.local",
        validation_alias=AliasChoices("DMS_REDIS_READ_HOST", "redis_read_host"),
    )
    redis_read_port: int = Field(
        6380, validation_alias=AliasChoices("DMS_REDIS_READ_PORT", "redis_read_port")
    )
    operator_token: str | None = Field(
        "changeme",
        validation_alias=AliasChoices("DMS_OPERATOR_TOKEN", "operator_token"),
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
