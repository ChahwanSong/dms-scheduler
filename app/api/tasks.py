from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.config import Settings, require_operator_token
from app.dependencies import get_settings, get_task_service
from app.models import (
    BlockResume,
    BlockToggle,
    CancelRequest,
    Priority,
    PriorityChangeRequest,
    TaskRequest,
)
from app.services.tasks import TaskService

router = APIRouter()


@router.post("/tasks", status_code=status.HTTP_201_CREATED)
async def create_task(
    request: TaskRequest,
    service: TaskService = Depends(get_task_service),
    settings: Settings = Depends(get_settings),
    x_dms_operator_token: str | None = Header(default=None, convert_underscores=False),
):
    blocked = await service.read_block()
    if blocked.get("blocked"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Scheduler is temporarily blocking frontend requests.",
        )

    if request.priority != Priority.low:
        try:
            require_operator_token(x_dms_operator_token, settings)
        except PermissionError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    try:
        record = await service.enqueue_task(request)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    return record


@router.post("/tasks/cancel")
async def cancel_task(
    request: CancelRequest,
    service: TaskService = Depends(get_task_service),
):
    try:
        record = await service.cancel_task(request)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return record


@router.get("/tasks/{task_id}")
async def get_task(task_id: str, service: TaskService = Depends(get_task_service)):
    try:
        summary = await service.get_task_summary(task_id)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return summary


@router.get("/queues/overview")
async def queues_overview(service: TaskService = Depends(get_task_service)):
    return await service.queue_overview()


@router.post("/admin/priority")
async def change_priority(
    request: PriorityChangeRequest,
    service: TaskService = Depends(get_task_service),
    settings=Depends(get_settings),
    x_dms_operator_token: str | None = Header(default=None, convert_underscores=False),
):
    try:
        require_operator_token(x_dms_operator_token, settings)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

    try:
        record = await service.change_priority(request)
    except KeyError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return record


@router.post("/admin/block")
async def toggle_block(
    request: BlockToggle,
    service: TaskService = Depends(get_task_service),
    settings: Settings = Depends(get_settings),
    x_dms_operator_token: str | None = Header(default=None, convert_underscores=False),
):
    try:
        require_operator_token(x_dms_operator_token, settings)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    return await service.toggle_block(request)


@router.post("/admin/resume")
async def resume_frontend(
    request: BlockResume,
    service: TaskService = Depends(get_task_service),
    settings: Settings = Depends(get_settings),
    x_dms_operator_token: str | None = Header(default=None, convert_underscores=False),
):
    try:
        require_operator_token(x_dms_operator_token, settings)
    except PermissionError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    return await service.resume_block(request)


@router.get("/admin/block")
async def read_block_state(service: TaskService = Depends(get_task_service)):
    return await service.read_block()


@router.get("/stats")
async def stats(service: TaskService = Depends(get_task_service)):
    return await service.stats()
