import logging
from fastapi import APIRouter, Depends, HTTPException, status

from ..models.schemas import CancelRequest, TaskRequest
from ..services.state_store import StateStore
from ..services.task_executor import TaskExecutor
from .deps import get_state_store

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)


@router.post("/task", status_code=status.HTTP_202_ACCEPTED)
async def submit_task(
    payload: TaskRequest,
    state_store: StateStore = Depends(get_state_store),
):
    if await state_store.is_blocked(payload.user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Requests are blocked")

    executor = TaskExecutor(state_store)
    state = await executor.handle_task(payload)
    logger.info("Accepted task %s", payload.task_id)
    return {"task_id": state.task_id, "status": state.status}


@router.post("/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_task(
    payload: CancelRequest,
    state_store: StateStore = Depends(get_state_store),
):
    executor = TaskExecutor(state_store)
    state = await executor.cancel_task(payload.task_id)
    if not state:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")
    return {"task_id": payload.task_id, "status": state.status}
