import pytest

from app.models.schemas import TaskRequest
from app.services.errors import TaskInvalidDirectoryError, TaskInvalidParametersError
from app.services.handlers.sync import SyncTaskHandler


@pytest.mark.anyio
@pytest.mark.parametrize(
    "src,dst,expected_path",
    [
        ("/not/allowed", "/pvs/data", "/not/allowed"),
        ("/pvs/data", "/not/allowed", "/not/allowed"),
    ],
)
async def test_validate_sync_invalid_directory_messages(src, dst, expected_path):
    handler = SyncTaskHandler(job_runner=None, state_store=None)
    request = TaskRequest(
        task_id="t1",
        service="sync",
        user_id="root",
        parameters={"src": src, "dst": dst},
    )

    with pytest.raises(TaskInvalidDirectoryError) as excinfo:
        await handler.validate(request)

    assert str(excinfo.value) == f"Invalid path to service 'sync': {expected_path}"


def test_validate_nsync_options_accepts_batch_without_value():
    handler = SyncTaskHandler(job_runner=None, state_store=None)

    errors = handler._validate_nsync_options("--dryrun --batch-files --delete")

    assert errors == []


def test_validate_nsync_options_rejects_invalid_flags_and_values():
    handler = SyncTaskHandler(job_runner=None, state_store=None)

    errors = handler._validate_nsync_options(
        "--role-mode invalid --imbalance-threshold nope --unknown"
    )

    assert any("--role-mode" in err for err in errors)
    assert any("--imbalance-threshold" in err for err in errors)
    assert any("Unsupported nsync option" in err for err in errors)


def test_validate_nsync_options_accepts_direct_flags():
    handler = SyncTaskHandler(job_runner=None, state_store=None)

    errors = handler._validate_nsync_options("--dryrun --direct --open-noatime")

    assert errors == []


def test_build_nsync_tokens_applies_default_batch_files_when_omitted_or_empty():
    handler = SyncTaskHandler(job_runner=None, state_store=None)

    tokens_without_batch = handler._build_nsync_tokens("--dryrun --delete")
    tokens_empty_batch = handler._build_nsync_tokens("--batch-files --dryrun")

    assert "--batch-files" in tokens_without_batch
    idx = tokens_without_batch.index("--batch-files")
    assert int(tokens_without_batch[idx + 1]) > 0

    assert tokens_empty_batch[0] == "--batch-files"
    assert int(tokens_empty_batch[1]) > 0
    assert "--direct" in tokens_without_batch
    assert "--open-noatime" in tokens_without_batch
    assert "--direct" in tokens_empty_batch
    assert "--open-noatime" in tokens_empty_batch


@pytest.mark.anyio
async def test_validate_sync_options_for_op_type_uses_specific_validator():
    handler = SyncTaskHandler(job_runner=None, state_store=None)

    await handler._validate_sync_options(
        task_id="t1",
        service="sync",
        op_type="nsync",
        options="--batch-files --dryrun",
    )

    with pytest.raises(TaskInvalidParametersError):
        await handler._validate_sync_options(
            task_id="t1",
            service="sync",
            op_type="nsync",
            options="--xattrs all",
        )
