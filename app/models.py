from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Priority(StrEnum):
    high = "high"
    mid = "mid"
    low = "low"


class TaskStatus(StrEnum):
    queued = "queued"
    dispatched = "dispatched"
    canceled = "canceled"


class TaskRequest(BaseModel):
    task_id: str
    service: str
    user_id: str
    parameters: dict[str, Any]
    priority: Priority = Field(default=Priority.low)


class CancelRequest(BaseModel):
    task_id: str
    service: str
    user_id: str


class TaskRecord(BaseModel):
    task_id: str
    service: str
    user_id: str
    parameters: dict[str, Any]
    priority: Priority
    status: TaskStatus
    enqueued_at: datetime
    updated_at: datetime


class QueuePosition(BaseModel):
    priority: Priority
    position_in_priority: int | None = None
    global_position: int | None = None


class QueueOverview(BaseModel):
    total: int
    high: int
    mid: int
    low: int


class TaskSummary(BaseModel):
    record: TaskRecord
    queue_position: QueuePosition | None = None
    overview: QueueOverview | None = None


class StatsSnapshot(BaseModel):
    queued: int
    dispatched: int
    canceled: int
    last_updated: datetime
    queue_overview: QueueOverview


class PriorityChangeRequest(BaseModel):
    task_id: str
    priority: Priority


class BlockToggle(BaseModel):
    blocked: bool
    reason: str | None = None


class BlockResume(BaseModel):
    reason: str | None = None
