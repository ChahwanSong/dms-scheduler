import logging
from fastapi import APIRouter, Depends, HTTPException, status

from ..models.schemas import CancelRequest, TaskRequest, TaskStatus
from ..services.errors import (
    TaskCancelForbiddenError,
    TaskInvalidDirectoryError,
    TaskInvalidParametersError,
    TaskNotFoundError,
    TaskNotMatchedError,
    TaskUnsupportedServiceError,
)
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
    try:
        state = await executor.handle_task(payload)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TaskNotMatchedError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except TaskUnsupportedServiceError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except TaskInvalidParametersError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except TaskInvalidDirectoryError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    logger.info(f"Accepted task {payload.task_id}")
    return {"task_id": payload.task_id, "status": TaskStatus.dispatching}


@router.post("/cancel", status_code=status.HTTP_202_ACCEPTED)
async def cancel_task(
    payload: CancelRequest,
    state_store: StateStore = Depends(get_state_store),
):
    executor = TaskExecutor(state_store)
    try:
        state = await executor.cancel_task(payload)
    except TaskNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except TaskCancelForbiddenError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    return {"task_id": payload.task_id, "status": state.status}
