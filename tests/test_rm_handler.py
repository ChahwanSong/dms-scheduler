import pytest

from app.models.schemas import TaskRequest, TaskStatus
from app.services.errors import TaskInvalidDirectoryError
from app.services.handlers.rm import RmTaskHandler
from app.services.kube import ExecResult


class DummyStateStore:
    def __init__(self):
        self.logs: list[tuple[str, str]] = []
        self.results = []

    async def append_log(self, task_id: str, message: str):
        self.logs.append((task_id, message))

    async def get_task(self, _task_id: str):
        class _State:
            status = TaskStatus.running

        return _State()

    async def set_result(self, task_id: str, result, message: str | None = None):
        self.results.append((task_id, result, message))
        return True


class DummyJobRunner:
    def __init__(self):
        self.exec_commands: list[list[str]] = []

    async def exec_in_pod_with_exit_code(self, _pod_name: str, command: list[str]):
        self.exec_commands.append(command)
        return ExecResult(stdout="", stderr="", exit_code=0)

    async def wait_for_completion(self, _label_selector: str, **_kwargs):
        return None

    async def list_pod_statuses(self, _label_selector: str):
        return {"pod-1": "Succeeded"}

    async def get_pod_logs(self, **_kwargs):
        return "line-1\nline-2"


@pytest.mark.anyio
async def test_check_format_rm_ignores_options_type():
    handler = RmTaskHandler(job_runner=None, state_store=None)
    request = TaskRequest(
        task_id="rm-1",
        service="rm",
        user_id="root",
        parameters={"path": "/pvs/data", "options": {"recursive": True}},
    )

    await handler._check_format_rm(request)


@pytest.mark.anyio
async def test_execute_logs_ignored_options_before_path_validation():
    state_store = DummyStateStore()
    handler = RmTaskHandler(job_runner=None, state_store=state_store)
    request = TaskRequest(
        task_id="rm-2",
        service="rm",
        user_id="root",
        parameters={"path": "/not/allowed/path", "options": ["-rf"]},
    )

    with pytest.raises(TaskInvalidDirectoryError):
        await handler.execute(request)

    assert state_store.logs == [
        (
            "rm-2",
            "Ignoring requested 'options'; rm uses fixed '--agreessive' mode",
        )
    ]


@pytest.mark.anyio
async def test_run_rm_updates_progress_and_saves_log(tmp_path, monkeypatch):
    state_store = DummyStateStore()
    job_runner = DummyJobRunner()
    handler = RmTaskHandler(job_runner=job_runner, state_store=state_store)

    monkeypatch.setattr("app.services.handlers.rm.K8S_DMS_LOG_DIRECTORY", str(tmp_path))

    result = await handler._run_rm(
        task_id="rm-3",
        label_selector="rm-job-id=rm-3",
        pod_name="pod-1",
        target_path="/pvs/data",
    )

    assert job_runner.exec_commands
    assert "--agreessive" in job_runner.exec_commands[0][2]
    assert state_store.results
    assert result.launcher_output
    assert (tmp_path / "rm-3.log").exists()
