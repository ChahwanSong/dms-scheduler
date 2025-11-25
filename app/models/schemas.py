from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_serializer

from ..core.timezone import now


class TaskStatus(str, Enum):
    """Enumeration of lifecycle stages for long-running tasks."""

    pending = "pending"
    dispatching = "dispatching"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"


class PriorityLevel(str, Enum):
    high = "high"
    low = "low"


class TaskRequest(BaseModel):
    task_id: str
    service: str
    user_id: str
    parameters: Dict[str, Any]


class CancelRequest(BaseModel):
    task_id: str
    service: str
    user_id: str


class PriorityRequest(BaseModel):
    task_id: str
    priority: PriorityLevel


class TaskResult(BaseModel):
    """Structured result payload attached to a task record."""

    pod_status: Optional[str] = None
    launcher_output: Optional[str] = None


class TaskRecord(BaseModel):
    """Serializable representation of a task stored in Redis."""

    task_id: str
    service: str
    user_id: str
    status: TaskStatus
    parameters: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=now)
    updated_at: datetime = Field(default_factory=now)
    logs: list[str] = Field(default_factory=list)
    result: TaskResult = Field(default_factory=TaskResult)
    priority: PriorityLevel = PriorityLevel.low

    @field_serializer("created_at", "updated_at")
    def serialize_datetimes(self, value: datetime) -> str:
        return value.isoformat()


class BlockStatus(BaseModel):
    blocked: bool = False
    blocked_users: list[str] = Field(default_factory=list)
