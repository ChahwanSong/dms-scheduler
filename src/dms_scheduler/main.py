import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .api import admin, tasks
from .core.logging import configure_logging
from .core.redis import RedisClient
from .core.settings import get_settings

configure_logging()
logger = logging.getLogger(__name__)
settings = get_settings()
redis_client: RedisClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    if redis_client:
        logger.info("Starting %s", settings.service_name)
        await redis_client.connect()
    yield
    if redis_client:
        logger.info("Stopping %s", settings.service_name)
        await redis_client.close()


def create_app(client: RedisClient | None = None) -> FastAPI:
    global redis_client
    redis_client = client or RedisClient()
    app = FastAPI(title="DMS Scheduler", version="0.1.0", lifespan=lifespan)

    app.include_router(tasks.router)
    app.include_router(admin.router)

    @app.get("/healthz")
    async def health_check():
        return {"status": "ok"}

    return app


app = create_app()
