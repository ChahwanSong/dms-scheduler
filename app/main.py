import argparse
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
import uvicorn

from .api import admin, tasks
from .core.logging import configure_logging
from .core.redis import RedisClient
from .core.settings import get_settings
from .core.timezone import now_in_configured_tz

configure_logging()
logger = logging.getLogger(__name__)
settings = get_settings()
redis_client: RedisClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    if redis_client:
        logger.info(f"Starting {settings.service_name}")
        await redis_client.connect()
    yield
    if redis_client:
        logger.info(f"Stopping {settings.service_name}")
        await redis_client.close()


def create_app(client: RedisClient | None = None) -> FastAPI:
    global redis_client
    redis_client = client or RedisClient()
    app = FastAPI(title="DMS Scheduler", version="0.1.0", lifespan=lifespan)

    app.include_router(tasks.router)
    app.include_router(admin.router)

    @app.get("/healthz")
    async def health_check():
        if redis_client:
            try:
                redis_ok = await redis_client.ping()
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Redis ping failed")
                raise HTTPException(status_code=503, detail="Redis unavailable") from exc
            if not redis_ok:
                raise HTTPException(status_code=503, detail="Redis unavailable")
            return {"status": "ok", "redis": "ok"}

        return {"status": "ok", "redis": "not_configured"}

    @app.get("/help")
    async def help_page():
        current_settings = get_settings()
        example_timestamp = now_in_configured_tz().isoformat()
        return {
            "service": current_settings.service_name,
            "timezone": current_settings.timezone,
            "log_format": "<iso-timestamp>,<message>",
            "example_log": f"{example_timestamp},Dispatching to scheduler",
        }

    return app


app = create_app()


def run() -> None:
    parser = argparse.ArgumentParser(description="Run the DMS Scheduler API server.")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0).")
    parser.add_argument("--port", type=int, default=9000, help="Port to bind (default: 9000).")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload for development.")
    args = parser.parse_args()

    uvicorn.run("app.main:app", host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    run()
