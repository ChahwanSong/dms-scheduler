"""Common task execution error types."""


class TaskNotFoundError(Exception):
    """Raised when a task_id does not exist in the shared store."""

    def __init__(self, task_id: str):
        self.task_id = task_id
        super().__init__(f"Task {task_id} not found in shared store")


class TaskNotMatchedError(Exception):
    """Raised when request fields do not match the stored task state."""

    def __init__(self, task_id: str, msg: str = None):
        self.task_id = task_id
        super().__init__(msg)


class TaskUnsupportedServiceError(Exception):
    """Raised when the requested service is not supported by the executor."""

    def __init__(self, task_id: str, msg: str):
        self.task_id = task_id
        super().__init__(msg)


class TaskInvalidParametersError(Exception):
    """Raised when the task parameters are invalid for the given service."""

    def __init__(self, task_id: str, service: str, errors: list[str]):
        self.task_id = task_id
        self.service = service
        self.errors = errors
        super().__init__(
            f"Invalid parameters for service {service!r}: " + "; ".join(errors)
        )


class TaskInvalidDirectoryError(Exception):
    """Raised when the directory values are invalid."""

    def __init__(self, task_id: str, dirpath: str, msg: str = None):
        self.task_id = task_id
        self.dirpath = dirpath
        if not msg:
            msg = "Invalid directory path"
        super().__init__(f"{msg}: {dirpath}")


class TaskTemplateRenderError(Exception):
    """Raised when a Kubernetes template cannot be rendered."""

    def __init__(self, template_path: str, exc: Exception):
        self.template_path = template_path
        super().__init__(f"Failed to render template {template_path}: {exc}")


class TaskJobError(Exception):
    """Raised for Kubernetes job lifecycle failures."""

    def __init__(self, task_id: str, message: str):
        self.task_id = task_id
        super().__init__(message)


__all__ = [
    "TaskInvalidDirectoryError",
    "TaskInvalidParametersError",
    "TaskJobError",
    "TaskNotFoundError",
    "TaskNotMatchedError",
    "TaskTemplateRenderError",
    "TaskUnsupportedServiceError",
]
