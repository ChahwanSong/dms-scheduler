from __future__ import annotations

from datetime import datetime

from app.models import (
    BlockResume,
    BlockToggle,
    CancelRequest,
    PriorityChangeRequest,
    QueueOverview,
    TaskRecord,
    TaskRequest,
    TaskStatus,
    TaskSummary,
)
from app.redis_client import RedisRepository


class TaskService:
    def __init__(self, repository: RedisRepository):
        self.repository = repository

    async def enqueue_task(self, request: TaskRequest) -> TaskRecord:
        existing = await self.repository.get_task(request.task_id)
        if existing:
            raise ValueError(f"Task {request.task_id} already exists")

        now = datetime.utcnow()
        record = TaskRecord(
            task_id=request.task_id,
            service=request.service,
            user_id=request.user_id,
            parameters=request.parameters,
            priority=request.priority,
            status=TaskStatus.queued,
            enqueued_at=now,
            updated_at=now,
        )
        await self.repository.save_task(record)
        await self.repository.enqueue(request.priority, record.task_id)
        await self.repository.increment_stat("queued")
        return record

    async def cancel_task(self, request: CancelRequest) -> TaskRecord:
        record = await self.repository.get_task(request.task_id)
        if not record:
            raise KeyError(f"Task {request.task_id} not found")

        if record.status == TaskStatus.canceled:
            return record

        if record.status == TaskStatus.queued:
            await self.repository.remove_from_queue(record.priority, record.task_id)

        record.status = TaskStatus.canceled
        record.updated_at = datetime.utcnow()
        await self.repository.save_task(record)
        await self.repository.increment_stat("canceled")
        return record

    async def dispatch_next(self) -> TaskRecord | None:
        task_id = await self.repository.pop_next_task()
        if not task_id:
            return None

        record = await self.repository.get_task(task_id)
        if not record:
            return None

        record.status = TaskStatus.dispatched
        record.updated_at = datetime.utcnow()
        await self.repository.save_task(record)
        await self.repository.increment_stat("dispatched")
        return record

    async def get_task_summary(self, task_id: str) -> TaskSummary:
        record = await self.repository.get_task(task_id)
        if not record:
            raise KeyError(f"Task {task_id} not found")

        position = await self.repository.find_position(task_id)
        overview = await self.repository.queue_lengths()
        return TaskSummary(record=record, queue_position=position, overview=overview)

    async def queue_overview(self) -> QueueOverview:
        return await self.repository.queue_lengths()

    async def change_priority(self, request: PriorityChangeRequest) -> TaskRecord:
        record = await self.repository.get_task(request.task_id)
        if not record:
            raise KeyError(f"Task {request.task_id} not found")

        if record.status != TaskStatus.queued:
            raise ValueError("Only queued tasks can change priority")

        if record.priority == request.priority:
            return record

        await self.repository.remove_from_queue(record.priority, record.task_id)
        await self.repository.enqueue(request.priority, record.task_id)
        record.priority = request.priority
        record.updated_at = datetime.utcnow()
        await self.repository.save_task(record)
        return record

    async def toggle_block(self, request: BlockToggle) -> dict:
        await self.repository.set_blocked(request.blocked, request.reason)
        return await self.repository.get_blocked()

    async def resume_block(self, request: BlockResume) -> dict:
        await self.repository.set_blocked(False, request.reason)
        return await self.repository.get_blocked()

    async def read_block(self) -> dict:
        return await self.repository.get_blocked()

    async def stats(self) -> dict:
        overview = await self.repository.queue_lengths()
        snapshot = await self.repository.read_stats(overview)
        return snapshot.model_dump()

    async def list_all_tasks(self) -> list[TaskRecord]:
        tasks = []
        for task_id in await self.repository.iter_tasks():
            record = await self.repository.get_task(task_id)
            if record:
                tasks.append(record)
        tasks.sort(key=lambda r: r.enqueued_at)
        return tasks
