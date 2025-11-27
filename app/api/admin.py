import logging
from fastapi import APIRouter, Depends, HTTPException, status

from ..models.schemas import PriorityLevel, PriorityRequest
from ..services.admin import AdminService
from ..services.state_store import StateStore
from .deps import get_state_store

router = APIRouter(prefix="/tasks", tags=["admin"])
logger = logging.getLogger(__name__)


@router.post("/priority", status_code=status.HTTP_202_ACCEPTED)
async def set_priority(
    payload: PriorityRequest,
    state_store: StateStore = Depends(get_state_store),
):
    admin = AdminService(state_store)
    updated = await admin.set_priority(payload.task_id, payload.priority)
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return {"task_id": payload.task_id, "priority": payload.priority}


@router.post("/block", status_code=status.HTTP_200_OK)
async def block_all(state_store: StateStore = Depends(get_state_store)):
    admin = AdminService(state_store)
    await admin.block_all()
    return {"blocked": True}


@router.post("/enable", status_code=status.HTTP_200_OK)
async def enable_all(state_store: StateStore = Depends(get_state_store)):
    admin = AdminService(state_store)
    await admin.enable_all()
    return {"blocked": False}


@router.post("/users/{user_id}/block", status_code=status.HTTP_200_OK)
async def block_user(user_id: str, state_store: StateStore = Depends(get_state_store)):
    admin = AdminService(state_store)
    await admin.block_user(user_id)
    return {"user_id": user_id, "blocked": True}


@router.post("/users/{user_id}/enable", status_code=status.HTTP_200_OK)
async def enable_user(user_id: str, state_store: StateStore = Depends(get_state_store)):
    admin = AdminService(state_store)
    await admin.enable_user(user_id)
    return {"user_id": user_id, "blocked": False}
