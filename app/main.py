from __future__ import annotations

import logging

from fastapi import FastAPI

from app.api import health, submitter, tasks
from app.config import get_settings
from app.dependencies import get_repository


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name)

    app.include_router(health.router)
    app.include_router(tasks.router)
    app.include_router(submitter.router)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:  # pragma: no cover - fastapi lifecycle
        repo = get_repository()
        await repo.close()
        logging.getLogger(__name__).info("Redis connections closed")

    return app


app = create_app()
