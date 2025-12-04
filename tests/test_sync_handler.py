import pytest

from app.models.schemas import TaskRequest
from app.services.errors import TaskInvalidDirectoryError
from app.services.handlers.sync import SyncTaskHandler


@pytest.mark.anyio
@pytest.mark.parametrize(
    "src,dst,expected_path",
    [
        ("/not/allowed", "/pvs/data", "/not/allowed"),
        ("/pvs/data", "/not/allowed", "/not/allowed"),
    ],
)
async def test_check_format_sync_invalid_directory_messages(src, dst, expected_path):
    handler = SyncTaskHandler(job_runner=None, state_store=None)
    request = TaskRequest(
        task_id="t1",
        service="sync",
        user_id="root",
        parameters={"src": src, "dst": dst},
    )

    with pytest.raises(TaskInvalidDirectoryError) as excinfo:
        await handler._check_format_sync(request)

    assert str(excinfo.value) == f"Invalid path to service 'sync': {expected_path}"
