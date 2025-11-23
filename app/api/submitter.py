from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import get_task_service
from app.services.tasks import TaskService

router = APIRouter()


@router.post("/submitter/next")
async def pop_next(service: TaskService = Depends(get_task_service)):
    record = await service.dispatch_next()
    if not record:
        raise HTTPException(status_code=status.HTTP_204_NO_CONTENT, detail="No tasks available")
    return record
