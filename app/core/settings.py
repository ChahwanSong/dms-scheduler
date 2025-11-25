from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    redis_write_host: str = Field("haproxy-redis.redis.svc.cluster.local", env="DMS_REDIS_WRITE_HOST")
    redis_write_port: int = Field(6379, env="DMS_REDIS_WRITE_PORT")
    redis_read_host: str = Field("haproxy-redis.redis.svc.cluster.local", env="DMS_REDIS_READ_HOST")
    redis_read_port: int = Field(6380, env="DMS_REDIS_READ_PORT")
    operator_token: str | None = Field("changeme", env="DMS_OPERATOR_TOKEN")
    service_name: str = Field("dms-scheduler", env="DMS_SERVICE_NAME")
    log_level: str = Field("INFO", env="DMS_LOG_LEVEL")

    class Config:
        env_file = ".env"
        case_sensitive = False


def get_settings() -> Settings:
    return Settings()
