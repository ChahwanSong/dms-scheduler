import pytest

from app.models.schemas import TaskRequest, TaskStatus
from app.services.errors import TaskInvalidParametersError
from app.services.handlers.hotcold import HotcoldTaskHandler
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
async def test_check_format_hotcold_rejects_non_string_options():
    handler = HotcoldTaskHandler(job_runner=None, state_store=None)
    request = TaskRequest(
        task_id="hotcold-1",
        service="hotcold",
        user_id="root",
        parameters={"path": "/pvs/data", "options": {"recursive": True}},
    )

    with pytest.raises(TaskInvalidParametersError):
        await handler._check_format_hotcold(request)


@pytest.mark.anyio
async def test_execute_validates_options_before_path_resolution():
    state_store = DummyStateStore()
    handler = HotcoldTaskHandler(job_runner=None, state_store=state_store)
    request = TaskRequest(
        task_id="hotcold-2",
        service="hotcold",
        user_id="root",
        parameters={"path": "/not/allowed/path", "options": "--bad-option"},
    )

    with pytest.raises(TaskInvalidParametersError):
        await handler.execute(request)


@pytest.mark.anyio
async def test_run_hotcold_updates_progress_and_saves_log(tmp_path, monkeypatch):
    state_store = DummyStateStore()
    job_runner = DummyJobRunner()
    handler = HotcoldTaskHandler(job_runner=job_runner, state_store=state_store)

    monkeypatch.setattr(
        "app.services.handlers.hotcold.K8S_DMS_LOG_DIRECTORY",
        str(tmp_path),
    )

    result = await handler._run_hotcold(
        task_id="hotcold-3",
        label_selector="hotcold-job-id=hotcold-3",
        pod_name="pod-1",
        target_path="/pvs/data",
        options="--days 100 --time-field atime --unit GB --exclude *tmp* --print",
    )

    assert job_runner.exec_commands
    assert "--days 100" in job_runner.exec_commands[0][2]
    assert "--output" in job_runner.exec_commands[0][2]
    assert str(tmp_path / "hotcold-3.hotcold.csv") in job_runner.exec_commands[0][2]
    assert state_store.results
    assert result.launcher_output
    assert (tmp_path / "hotcold-3.log").exists()


@pytest.mark.anyio
async def test_run_hotcold_logs_output_override(tmp_path, monkeypatch):
    state_store = DummyStateStore()
    job_runner = DummyJobRunner()
    handler = HotcoldTaskHandler(job_runner=job_runner, state_store=state_store)

    monkeypatch.setattr(
        "app.services.handlers.hotcold.K8S_DMS_LOG_DIRECTORY",
        str(tmp_path),
    )

    await handler._run_hotcold(
        task_id="hotcold-4",
        label_selector="hotcold-job-id=hotcold-4",
        pod_name="pod-1",
        target_path="/pvs/data",
        options="--days 10 --output /tmp/from-user.csv",
    )

    assert any(
        "Overriding requested hotcold --output option(s)" in message
        and "hotcold-4.hotcold.csv" in message
        for _task_id, message in state_store.logs
    )


def test_validate_hotcold_options_rejects_unsupported_flag():
    handler = HotcoldTaskHandler(job_runner=None, state_store=None)

    errors = handler._validate_hotcold_options("--days 30 --unknown value")

    assert errors
    assert "Unsupported hotcold option" in errors[0]
