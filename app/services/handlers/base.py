"""Base protocol for task handlers."""

from ...models.schemas import TaskRequest, TaskResult


class BaseTaskHandler:
    async def validate(self, request: TaskRequest) -> None:
        raise NotImplementedError

    async def execute(self, request: TaskRequest) -> TaskResult:
        raise NotImplementedError


__all__ = ["BaseTaskHandler"]
