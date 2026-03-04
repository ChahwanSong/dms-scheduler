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


@pytest.mark.anyio
async def test_has_node_with_true_labels_returns_true_when_single_node_matches(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    async def _filtered(label_keys):
        assert tuple(label_keys) == ("src", "dst")
        return {
            "node-a": {"src": "true", "dst": "false"},
            "node-b": {"src": "true", "dst": "true"},
        }

    monkeypatch.setattr(runner, "get_node_label_map_filtered", _filtered)

    assert await runner.has_node_with_true_labels(["src", "dst"], 1) is True


@pytest.mark.anyio
async def test_has_node_with_true_labels_returns_false_for_empty_input():
    runner = VolcanoJobRunner(namespace="default")

    assert await runner.has_node_with_true_labels([], 1) is False




@pytest.mark.anyio
async def test_has_nodes_covering_true_labels_allows_different_nodes(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    async def _filtered(label_keys):
        assert tuple(label_keys) == ("src", "dst")
        return {
            "node-a": {"src": "true", "dst": "false"},
            "node-b": {"src": "false", "dst": "true"},
        }

    monkeypatch.setattr(runner, "get_node_label_map_filtered", _filtered)

    assert await runner.has_nodes_covering_true_labels({"src": 1, "dst": 1}) is True


@pytest.mark.anyio
async def test_has_nodes_covering_true_labels_returns_false_for_empty_input():
    runner = VolcanoJobRunner(namespace="default")

    assert await runner.has_nodes_covering_true_labels({}) is False


@pytest.mark.anyio
async def test_has_nodes_covering_true_labels_returns_false_when_missing_label(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    async def _filtered(label_keys):
        return {"node-a": {"src": "true", "dst": "false"}}

    monkeypatch.setattr(runner, "get_node_label_map_filtered", _filtered)

    assert await runner.has_nodes_covering_true_labels({"src": 1, "dst": 1}) is False


@pytest.mark.anyio
async def test_has_node_with_true_labels_checks_minimum_matching_nodes(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    async def _filtered(label_keys):
        return {
            "node-a": {"src": "true", "dst": "true"},
            "node-b": {"src": "true", "dst": "true"},
        }

    monkeypatch.setattr(runner, "get_node_label_map_filtered", _filtered)

    assert await runner.has_node_with_true_labels(["src", "dst"], 3) is False


@pytest.mark.anyio
async def test_has_nodes_covering_true_labels_checks_per_label_minimum(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    async def _filtered(label_keys):
        return {
            "node-a": {"src": "true", "dst": "false"},
            "node-b": {"src": "true", "dst": "true"},
            "node-c": {"src": "false", "dst": "true"},
        }

    monkeypatch.setattr(runner, "get_node_label_map_filtered", _filtered)

    assert await runner.has_nodes_covering_true_labels({"src": 2, "dst": 2}) is True
    assert await runner.has_nodes_covering_true_labels({"src": 3, "dst": 2}) is False

@pytest.mark.anyio
async def test_wait_for_pods_scheduled_fails_when_schedule_precheck_is_false(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector):
            raise AssertionError("pod listing should not run when precheck fails")

    monkeypatch.setattr(runner, "_require_clients", lambda: (_Core(), object()))

    async def _always_false(label_keys, worker_count):
        assert label_keys == ["src", "dst"]
        assert worker_count == 1
        return False

    monkeypatch.setattr(runner, "has_node_with_true_labels", _always_false)

    with pytest.raises(TaskJobError) as excinfo:
        await runner.wait_for_pods_scheduled(
            task_id="t1",
            label_selector="job=a",
            expected=1,
            timeout=5,
            schedule_precheck=lambda: runner.has_node_with_true_labels(["src", "dst"], 1),
            schedule_precheck_error="No node has all required labels set to true: ['src', 'dst']",
        )

    assert "Scheduling precheck failed" in str(excinfo.value)
    assert "No node has all required labels set to true" in str(excinfo.value)


@pytest.mark.anyio
async def test_wait_for_pods_scheduled_accepts_lambda_with_inputs(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    required_labels = ["src", "dst"]
    required_worker_count = 1

    async def _has_labels(label_keys, worker_count):
        return label_keys == required_labels and worker_count == required_worker_count

    monkeypatch.setattr(runner, "has_node_with_true_labels", _has_labels)

    scheduled_pod = V1Pod(metadata=V1ObjectMeta(name="p-1"))
    scheduled_pod.spec = type("_Spec", (), {"node_name": "n-1"})()

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector):
            return type("_Result", (), {"items": [scheduled_pod]})

    monkeypatch.setattr(runner, "_require_clients", lambda: (_Core(), object()))

    pods = await runner.wait_for_pods_scheduled(
        task_id="t1",
        label_selector="job=a",
        expected=1,
        timeout=5,
        schedule_precheck=lambda: runner.has_node_with_true_labels(required_labels, required_worker_count),
    )

    assert [p.metadata.name for p in pods] == ["p-1"]


@pytest.mark.anyio
async def test_wait_for_pods_scheduled_uses_configured_poll_interval(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector):
            return type("_Result", (), {"items": []})

    monkeypatch.setattr(runner, "_require_clients", lambda: (_Core(), object()))

    class _StopLoop(RuntimeError):
        pass

    async def _sleep(seconds):
        assert seconds == 10
        raise _StopLoop()

    monkeypatch.setattr("app.services.kube.asyncio.sleep", _sleep)

    with pytest.raises(_StopLoop):
        await runner.wait_for_pods_scheduled(
            task_id="t1",
            label_selector="job=a",
            expected=1,
            timeout=60,
        )


@pytest.mark.anyio
async def test_wait_for_pods_ready_uses_configured_poll_interval(monkeypatch):
    runner = VolcanoJobRunner(namespace="default")

    class _Core:
        def list_namespaced_pod(self, namespace, label_selector):
            return type("_Result", (), {"items": []})

    monkeypatch.setattr(runner, "_require_clients", lambda: (_Core(), object()))

    class _StopLoop(RuntimeError):
        pass

    async def _sleep(seconds):
        assert seconds == 5
        raise _StopLoop()

    monkeypatch.setattr("app.services.kube.asyncio.sleep", _sleep)

    with pytest.raises(_StopLoop):
        await runner.wait_for_pods_ready(
            task_id="t1",
            label_selector="job=a",
            expected=1,
            timeout=60,
        )
