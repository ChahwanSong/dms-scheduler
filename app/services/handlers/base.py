"""Base protocol for task handlers."""

from ...models.schemas import CancelRequest, TaskRequest, TaskResult


class BaseTaskHandler:
    async def validate(self, request: TaskRequest) -> None:
        raise NotImplementedError

    async def execute(self, request: TaskRequest) -> TaskResult:
        raise NotImplementedError

    async def cancel(self, request: CancelRequest, state=None) -> None:
        """Request cancellation for a task.

        Default implementation is a no-op for handlers that do not support
        explicit cancellation.
        """

        return None


__all__ = ["BaseTaskHandler"]
