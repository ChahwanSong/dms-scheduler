from kubernetes.client import V1ObjectMeta, V1Pod

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
