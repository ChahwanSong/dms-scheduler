import pytest
from kubernetes.client import V1ObjectMeta, V1Pod

from app.services.errors import TaskJobError
from app.services.handlers.sync import SyncTaskHandler
from app.services.kube import VolcanoJobRunner


def _pod(name: str, labels: dict[str, str] | None = None) -> V1Pod:
    return V1Pod(metadata=V1ObjectMeta(name=name, labels=labels or {}))


def test_infer_expected_pod_count_prefers_min_available():
    job = {
        "spec": {
            "minAvailable": "6",
            "tasks": [{"name": "master", "replicas": 1}, {"name": "worker", "replicas": 5}],
        }
    }

    expected = VolcanoJobRunner.infer_expected_pod_count(job, default=1)
    assert expected == 6


def test_infer_expected_pod_count_uses_task_replica_sum_without_min_available():
    job = {
        "spec": {
            "tasks": [{"name": "master", "replicas": "1"}, {"name": "worker", "replicas": "5"}],
        }
    }

    expected = VolcanoJobRunner.infer_expected_pod_count(job, default=1)
    assert expected == 6


def test_infer_expected_pod_count_falls_back_to_default_for_invalid_spec():
    job = {"spec": {"minAvailable": 0, "tasks": []}}
    expected = VolcanoJobRunner.infer_expected_pod_count(job, default=3)
    assert expected == 3


def test_pick_master_pod_prefers_task_spec_label_then_name():
    pods = [
        _pod("sync-worker-0", labels={"volcano.sh/task-spec": "worker"}),
        _pod("sync-main-0", labels={"volcano.sh/task-spec": "master"}),
        _pod("sync-master-0"),
    ]

    selected = SyncTaskHandler._pick_master_pod("t-sync", pods)
    assert selected.metadata.name == "sync-main-0"


def test_log_wait_status_change_no_pods_logs_info_once_then_debug(caplog):
    caplog.set_level("DEBUG")

    no_pods_info_logged = VolcanoJobRunner._log_wait_status_change(
        status_kind="scheduling",
        task_id="430",
        label_selector="job=a",
        summary="No pods observed yet",
        no_pods_info_logged=False,
    )
    no_pods_info_logged = VolcanoJobRunner._log_wait_status_change(
        status_kind="scheduling",
        task_id="430",
        label_selector="job=a",
        summary="No pods observed yet",
        no_pods_info_logged=no_pods_info_logged,
    )

    assert no_pods_info_logged is True
    assert "No pods observed yet" in caplog.text
    assert "[Task 430]" in caplog.text
    info_logs = [r for r in caplog.records if r.levelname == "INFO"]
    debug_logs = [r for r in caplog.records if r.levelname == "DEBUG"]
    assert len(info_logs) == 1
    assert len(debug_logs) == 1


@pytest.mark.anyio
async def test_wait_for_pods_ready_cancellation_message_and_metric(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")
    with runner._WAIT_METRICS_LOCK:
        runner._WAIT_METRICS.clear()

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector):
            return type("_Result", (), {"items": []})

    monkeypatch.setattr(runner, "_require_clients", lambda: (_Core(), object()))

    async def _cancel():
        raise TaskJobError("t1", "cancel")

    with pytest.raises(TaskJobError) as excinfo:
        await runner.wait_for_pods_ready(
            task_id="t1",
            label_selector="job=a",
            expected=1,
            timeout=1,
            should_continue=_cancel,
        )

    assert "aborted due to cancellation" in str(excinfo.value)
    assert "Last pod summary" not in str(excinfo.value)
    assert runner.get_wait_metrics_snapshot()["pods_wait_aborted_total"] == 1
