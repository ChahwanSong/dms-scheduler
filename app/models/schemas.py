from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field

from ..core.timezone import now_in_configured_tz


class TaskStatus(str, Enum):
    accepted = "accepted"
    running = "running"
    completed = "completed"
    failed = "failed"
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
    pod_status: Optional[str] = None
    launcher_output: Optional[str] = None


class TaskState(BaseModel):
    task_id: str
    service: str
    user_id: str
    parameters: Dict[str, Any]
    status: TaskStatus
    updated_at: datetime = Field(default_factory=now_in_configured_tz)
    logs: list[str] = Field(default_factory=list)
    result: Optional[TaskResult] = None
    priority: PriorityLevel = PriorityLevel.low


class BlockStatus(BaseModel):
    blocked: bool = False
    blocked_users: list[str] = Field(default_factory=list)
