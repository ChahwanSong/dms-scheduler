"""Kubernetes and Volcano helpers for task execution."""

import asyncio
import logging
import time
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple, Any, Mapping

from kubernetes import client, config
from kubernetes.client import ApiException, V1DeleteOptions, V1Pod
from kubernetes.config import ConfigException
from kubernetes.stream import stream

from .errors import TaskJobError

logger = logging.getLogger(__name__)


class KubernetesClients:
    """Lazy loader for Kubernetes API clients."""

    def __init__(self, namespace: str):
        self.namespace = namespace
        self.core_api: Optional[client.CoreV1Api] = None
        self.custom_api: Optional[client.CustomObjectsApi] = None
        self.config_source: Optional[str] = None

    def load(self) -> None:
        if self.core_api and self.custom_api:
            return

        try:
            config.load_incluster_config()
            self.config_source = "in-cluster"
        except ConfigException:
            config.load_kube_config()
            self.config_source = "kubeconfig"

        self.core_api = client.CoreV1Api()
        self.custom_api = client.CustomObjectsApi()
        logger.info(f"Kubernetes config loaded ({self.config_source})")

    def new_core_api(self) -> client.CoreV1Api:
        """Return a fresh CoreV1Api with an isolated ApiClient.

        The Kubernetes Python client's ``stream`` helper monkey-patches the
        ``ApiClient.request`` method on the instance it receives. Sharing the
        same ApiClient between streaming calls and regular HTTP calls can cause
        concurrent requests to be routed through the websocket handler, which
        fails with ``Handshake status 200 OK``. To avoid this, use a dedicated
        ApiClient for operations that rely on ``stream``.
        """

        self.load()

        # ``get_default_copy`` provides a copy of the currently loaded
        # configuration, ensuring the returned ApiClient does not share the
        # instance that might be monkey-patched by ``stream``.
        configuration = client.Configuration.get_default_copy()
        return client.CoreV1Api(api_client=client.ApiClient(configuration=configuration))


@dataclass
class PodMountCheckResult:
    name: str
    output: str


@dataclass
class PodPathCheckResult:
    name: str
    output: str


@dataclass
class ExecResult:
    stdout: str
    stderr: str
    exit_code: Optional[int]


class VolcanoJobRunner:
    """Utility wrapper for creating, monitoring, and cleaning Volcano jobs."""

    _FATAL_CONTAINER_WAITING_REASONS = frozenset(
        {
            "CrashLoopBackOff",
            "CreateContainerConfigError",
            "CreateContainerError",
            "ErrImagePull",
            "ImageInspectError",
            "ImagePullBackOff",
            "InvalidImageName",
            "RunContainerError",
        }
    )

    def __init__(self, namespace: str):
        self.namespace = namespace
        self.clients = KubernetesClients(namespace)

    def _require_clients(self) -> Tuple[client.CoreV1Api, client.CustomObjectsApi]:
        self.clients.load()
        assert self.clients.core_api and self.clients.custom_api
        return self.clients.core_api, self.clients.custom_api

    async def create_job(self, body: dict) -> None:
        core_api, custom_api = self._require_clients()
        job_name = body.get("metadata", {}).get("name", "<unknown>")

        def _create():
            return custom_api.create_namespaced_custom_object(
                group="batch.volcano.sh",
                version="v1alpha1",
                namespace=self.namespace,
                plural="jobs",
                body=body,
            )

        try:
            await asyncio.to_thread(_create)
            logger.info(f"Created VolcanoJob {job_name}")
        except ApiException as exc:  # pragma: no cover - network side effects
            logger.error(f"Failed to create VolcanoJob {job_name}: {exc}")
            raise TaskJobError(job_name, f"Failed to create Volcano job: {exc}") from exc

    async def delete_job(self, job_name: str) -> None:
        _, custom_api = self._require_clients()

        def _delete():
            return custom_api.delete_namespaced_custom_object(
                group="batch.volcano.sh",
                version="v1alpha1",
                namespace=self.namespace,
                plural="jobs",
                name=job_name,
                body=V1DeleteOptions(propagation_policy="Foreground"),
            )

        try:
            await asyncio.to_thread(_delete)
            logger.info(f"Deleted VolcanoJob {job_name}")
        except ApiException as exc:  # pragma: no cover - network side effects
            if exc.status == 404:
                logger.warning(f"VolcanoJob {job_name} already removed")
                return
            logger.error(f"Failed to delete VolcanoJob {job_name}: {exc}")
            raise TaskJobError(job_name, f"Failed to delete job: {exc}") from exc

    @staticmethod
    def infer_expected_pod_count(job_body: Mapping[str, Any], default: int = 1) -> int:
        """Infer expected pod count from VolcanoJob spec."""

        if default < 1:
            default = 1

        spec = job_body.get("spec")
        if not isinstance(spec, Mapping):
            return default

        min_available = VolcanoJobRunner._to_positive_int(spec.get("minAvailable"))
        if min_available is not None:
            return min_available

        tasks = spec.get("tasks")
        if not isinstance(tasks, list):
            return default

        replicas_total = 0
        for task in tasks:
            if not isinstance(task, Mapping):
                continue

            replicas = VolcanoJobRunner._to_positive_int(task.get("replicas"))
            replicas_total += replicas if replicas is not None else 1

        return replicas_total if replicas_total > 0 else default

    async def wait_for_pods_scheduled(
        self, label_selector: str, expected: int, timeout: int = 120
    ) -> list[V1Pod]:
        core_api, _ = self._require_clients()

        def _wait() -> list[V1Pod]:
            deadline = time.time() + timeout
            last_seen: list[V1Pod] = []
            last_summary = "No pods observed yet"

            while time.time() < deadline:
                pods = core_api.list_namespaced_pod(
                    namespace=self.namespace,
                    label_selector=label_selector,
                ).items
                last_seen = pods
                last_summary = self._summarize_pods(pods)

                failed = [p for p in pods if self._pod_phase(p) == "Failed"]
                if failed:
                    names = ", ".join(self._pod_name(p) for p in failed)
                    raise TaskJobError(
                        label_selector,
                        f"Pods failed before scheduling: {names}. {last_summary}",
                    )

                scheduled = [p for p in pods if self._is_pod_scheduled(p)]
                if len(scheduled) >= expected:
                    return scheduled

                time.sleep(1)

            names = [self._pod_name(p) for p in last_seen]
            raise TaskJobError(
                label_selector,
                (
                    "Timed out waiting for pods to be scheduled by Volcano "
                    f"(expected {expected}). Last seen: {names}. {last_summary}"
                ),
            )

        return await asyncio.to_thread(_wait)

    async def wait_for_pods_ready(
        self, label_selector: str, expected: int, timeout: int = 120
    ) -> list[V1Pod]:
        core_api, _ = self._require_clients()

        def _wait() -> list[V1Pod]:
            deadline = time.time() + timeout
            last_seen: list[V1Pod] = []
            last_summary = "No pods observed yet"

            while time.time() < deadline:
                pods = core_api.list_namespaced_pod(
                    namespace=self.namespace, label_selector=label_selector
                ).items
                last_seen = pods
                last_summary = self._summarize_pods(pods)

                ready = [p for p in pods if self._is_pod_ready(p)]
                failed = [p for p in pods if self._pod_phase(p) == "Failed"]

                if failed:
                    names = ", ".join(self._pod_name(p) for p in failed)
                    raise TaskJobError(
                        label_selector, f"Pods failed before ready: {names}. {last_summary}"
                    )

                fatal_issue = self._find_fatal_runtime_issue(pods)
                if fatal_issue:
                    raise TaskJobError(
                        label_selector,
                        (
                            "Pods entered fatal runtime state before readiness: "
                            f"{fatal_issue}. {last_summary}"
                        ),
                    )

                if len(ready) >= expected:
                    return ready

                time.sleep(1)

            names = [self._pod_name(p) for p in last_seen]
            raise TaskJobError(
                label_selector,
                (
                    "Timed out waiting for pods to become Ready "
                    f"(expected {expected}). Last seen: {names}. {last_summary}"
                ),
            )

        return await asyncio.to_thread(_wait)

    async def wait_for_completion(
        self,
        label_selector: str,
        success_phases: Iterable[str] = ("Succeeded",),
        failure_phases: Iterable[str] = ("Failed",),
        timeout: int = 900,
    ) -> list[V1Pod]:
        core_api, _ = self._require_clients()
        success = set(success_phases)
        failure = set(failure_phases)

        def _wait() -> list[V1Pod]:
            deadline = time.time() + timeout
            last_seen: list[V1Pod] = []

            while time.time() < deadline:
                pods = core_api.list_namespaced_pod(
                    namespace=self.namespace, label_selector=label_selector
                ).items
                last_seen = pods

                if not pods:
                    time.sleep(1)
                    continue

                failed = [p for p in pods if p.status.phase in failure]
                if failed:
                    names = ", ".join(p.metadata.name for p in failed)
                    raise TaskJobError(label_selector, f"Pods failed: {names}")

                completed = [p for p in pods if p.status.phase in success]
                if completed:
                    return completed

                time.sleep(2)

            names = [p.metadata.name for p in last_seen]
            raise TaskJobError(
                label_selector,
                f"Timed out waiting for completion. Last seen: {names}",
            )

        return await asyncio.to_thread(_wait)

    async def exec_in_pod(
        self, pod_name: str, command: list[str], container: Optional[str] = None
    ) -> str:
        # Use a dedicated ApiClient for streaming calls to avoid monkey-patching
        # the shared client used by other operations running concurrently.
        core_api = self.clients.new_core_api()

        def _exec() -> str:
            return stream(
                core_api.connect_get_namespaced_pod_exec,
                pod_name,
                self.namespace,
                command=command,
                container=container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )

        try:
            return await asyncio.to_thread(_exec)
        except ApiException as exc:  # pragma: no cover - network side effects
            raise TaskJobError(pod_name, f"Failed to exec in pod: {exc}") from exc

    async def exec_in_pod_with_exit_code(
        self, pod_name: str, command: list[str], container: Optional[str] = None
    ) -> ExecResult:
        core_api = self.clients.new_core_api()

        def _exec() -> ExecResult:
            resp = stream(
                core_api.connect_get_namespaced_pod_exec,
                pod_name,
                self.namespace,
                command=command,
                container=container,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=False,
            )

            stdout_parts: list[str] = []
            stderr_parts: list[str] = []

            while resp.is_open():
                resp.update(timeout=1)

                if resp.peek_stdout():
                    stdout_parts.append(resp.read_stdout())

                if resp.peek_stderr():
                    stderr_parts.append(resp.read_stderr())

                if resp.returncode is not None:
                    break

            resp.close()

            return ExecResult(
                stdout="".join(stdout_parts),
                stderr="".join(stderr_parts),
                exit_code=resp.returncode,
            )

        try:
            return await asyncio.to_thread(_exec)
        except ApiException as exc:  # pragma: no cover - network side effects
            raise TaskJobError(pod_name, f"Failed to exec in pod: {exc}") from exc

    async def get_pod_logs(
        self, pod_name: str, container: Optional[str] = None, tail_lines: Optional[int] = None, request_timeout: int = 10,
    ) -> str:
        core_api, _ = self._require_clients()

        # by default, get lines at tail
        if tail_lines is None:
            tail_lines = 100

        def _logs() -> str:
            return core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                container=container,
                tail_lines=tail_lines,
                _request_timeout=request_timeout,
            )

        try:
            return await asyncio.to_thread(_logs)
        except ApiException as exc:  # pragma: no cover - network side effects
            raise TaskJobError(pod_name, f"Failed to fetch pod logs: {exc}") from exc

    async def list_pod_statuses(self, label_selector: str) -> dict[str, str]:
        try:
            core_api, _ = self._require_clients()
        except TaskJobError:
            return {}

        def _list_pods() -> list[V1Pod]:
            return core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector=label_selector,
            ).items

        try:
            pods = await asyncio.to_thread(_list_pods)
        except ApiException as exc:  # pragma: no cover - network side effects
            logger.warning("Failed to list pods for %s: %s", label_selector, exc)
            return {}

        return {pod.metadata.name: pod.status.phase or "Unknown" for pod in pods}

    async def get_node_label_map(self) -> dict[str, dict[str, str]]:
        """Return mapping of node_name -> labels.

        Example:
            {
              "ion2401": {"kubernetes.io/hostname": "ion2401", "mount-A": "true", ...},
              ...
            }
        """
        core_api, _ = self._require_clients()

        def _list_nodes() -> dict[str, dict[str, str]]:
            nodes = core_api.list_node().items
            out: dict[str, dict[str, str]] = {}

            for n in nodes:
                name = (n.metadata and n.metadata.name) or ""
                if not name:
                    continue
                labels = (n.metadata and n.metadata.labels) or {}
                # kubernetes client가 dict[str,str]로 주지만 안전하게 copy
                out[name] = dict(labels)

            return out

        try:
            return await asyncio.to_thread(_list_nodes)
        except ApiException as exc:  # pragma: no cover - network side effects
            logger.warning("Failed to list nodes: %s", exc)
            return {}

    # 필요하면 특정 label만 필터링 버전도 같이 (옵션)
    async def get_node_label_map_filtered(
        self, label_keys: Iterable[str]
    ) -> dict[str, dict[str, str]]:
        """Return node_name -> subset of labels for given keys."""
        label_keys_set = set(label_keys)
        full = await self.get_node_label_map()
        return {
            node: {k: v for k, v in labels.items() if k in label_keys_set}
            for node, labels in full.items()
        }

    @staticmethod
    def _is_pod_ready(pod: V1Pod) -> bool:
        pod_status = pod.status
        if not pod_status or pod_status.phase != "Running":
            return False
        if not pod_status.container_statuses:
            return False
        return all(cs.ready for cs in pod_status.container_statuses)

    @staticmethod
    def _pod_phase(pod: V1Pod) -> str:
        if pod.status and pod.status.phase:
            return pod.status.phase
        return "Unknown"

    @staticmethod
    def _pod_name(pod: V1Pod) -> str:
        if pod.metadata and pod.metadata.name:
            return pod.metadata.name
        return "<unknown-pod>"

    @staticmethod
    def _to_positive_int(value: Any) -> Optional[int]:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return None
        if parsed < 1:
            return None
        return parsed

    @staticmethod
    def _is_pod_scheduled(pod: V1Pod) -> bool:
        pod_spec = pod.spec
        if pod_spec and pod_spec.node_name:
            return True

        condition = VolcanoJobRunner._get_pod_condition(pod, "PodScheduled")
        return bool(condition and condition.status == "True")

    @staticmethod
    def _get_pod_condition(pod: V1Pod, condition_type: str) -> Optional[Any]:
        pod_status = pod.status
        if not pod_status:
            return None

        conditions = pod_status.conditions or []
        for condition in conditions:
            if condition.type == condition_type:
                return condition
        return None

    @staticmethod
    def _shorten_message(message: str, limit: int = 140) -> str:
        compact = " ".join(message.split())
        if len(compact) <= limit:
            return compact
        return f"{compact[: limit - 3]}..."

    @classmethod
    def _summarize_pods(cls, pods: list[V1Pod]) -> str:
        if not pods:
            return "No pods observed yet"

        phases = Counter(cls._pod_phase(pod) for pod in pods)
        queue_waiting = 0
        runtime_waiting = 0
        scheduling_reasons: Counter[str] = Counter()
        waiting_reasons: Counter[str] = Counter()

        for pod in pods:
            if not cls._is_pod_scheduled(pod):
                queue_waiting += 1
                scheduled_condition = cls._get_pod_condition(pod, "PodScheduled")
                if scheduled_condition and scheduled_condition.status == "False":
                    reason = scheduled_condition.reason or "PodScheduled=False"
                    message = (scheduled_condition.message or "").strip()
                    if message:
                        reason = f"{reason}: {cls._shorten_message(message)}"
                    scheduling_reasons[reason] += 1

            if cls._is_pod_scheduled(pod) and not cls._is_pod_ready(pod):
                runtime_waiting += 1

            for reason in cls._iter_container_waiting_reasons(pod):
                waiting_reasons[reason] += 1

        ready_count = sum(1 for pod in pods if cls._is_pod_ready(pod))
        scheduled_count = sum(1 for pod in pods if cls._is_pod_scheduled(pod))

        return (
            f"pods={len(pods)}, scheduled={scheduled_count}, ready={ready_count}, "
            f"queue_waiting={queue_waiting}, runtime_waiting={runtime_waiting}, "
            f"phases={cls._format_counter(phases)}, "
            f"scheduling_reasons={cls._format_counter(scheduling_reasons)}, "
            f"container_waiting={cls._format_counter(waiting_reasons)}"
        )

    @staticmethod
    def _iter_all_container_statuses(pod: V1Pod) -> Iterable[Any]:
        pod_status = pod.status
        if not pod_status:
            return ()

        combined: list[Any] = []
        if pod_status.init_container_statuses:
            combined.extend(pod_status.init_container_statuses)
        if pod_status.container_statuses:
            combined.extend(pod_status.container_statuses)
        return combined

    @classmethod
    def _iter_container_waiting_reasons(cls, pod: V1Pod) -> list[str]:
        reasons: list[str] = []
        for container_status in cls._iter_all_container_statuses(pod):
            state = container_status.state
            if not state or not state.waiting or not state.waiting.reason:
                continue
            reasons.append(state.waiting.reason)
        return reasons

    @classmethod
    def _find_fatal_runtime_issue(cls, pods: list[V1Pod]) -> Optional[str]:
        for pod in pods:
            pod_name = cls._pod_name(pod)
            for container_status in cls._iter_all_container_statuses(pod):
                container_name = getattr(container_status, "name", "<unknown>")
                state = container_status.state
                if not state:
                    continue

                waiting_state = state.waiting
                if waiting_state and waiting_state.reason in cls._FATAL_CONTAINER_WAITING_REASONS:
                    message = waiting_state.message or ""
                    message_suffix = (
                        f": {cls._shorten_message(message)}" if message else ""
                    )
                    return (
                        f"{pod_name}/{container_name} waiting={waiting_state.reason}"
                        f"{message_suffix}"
                    )

                terminated_state = state.terminated
                if terminated_state and terminated_state.exit_code not in (0, None):
                    reason = terminated_state.reason or "Terminated"
                    return (
                        f"{pod_name}/{container_name} terminated={reason}"
                        f"(exit_code={terminated_state.exit_code})"
                    )

        return None

    @staticmethod
    def _format_counter(counter: Counter[str], limit: int = 3) -> str:
        if not counter:
            return "-"
        return ", ".join(
            f"{label}({count})" for label, count in counter.most_common(limit)
        )


__all__ = [
    "KubernetesClients",
    "PodMountCheckResult",
    "PodPathCheckResult",
    "ExecResult",
    "VolcanoJobRunner",
]
