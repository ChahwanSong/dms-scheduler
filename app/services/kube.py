"""Kubernetes and Volcano helpers for task execution."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

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


class VolcanoJobRunner:
    """Utility wrapper for creating, monitoring, and cleaning Volcano jobs."""

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

    async def wait_for_pods_ready(
        self, label_selector: str, expected: int, timeout: int = 120
    ) -> list[V1Pod]:
        core_api, _ = self._require_clients()

        def _wait() -> list[V1Pod]:
            deadline = time.time() + timeout
            last_seen: list[V1Pod] = []

            while time.time() < deadline:
                pods = core_api.list_namespaced_pod(
                    namespace=self.namespace, label_selector=label_selector
                ).items
                last_seen = pods

                ready = [p for p in pods if self._is_pod_ready(p)]
                failed = [p for p in pods if p.status.phase == "Failed"]

                if failed:
                    names = ", ".join(p.metadata.name for p in failed)
                    raise TaskJobError(label_selector, f"Pods failed: {names}")

                if len(ready) >= expected:
                    return ready

                time.sleep(1)

            names = [p.metadata.name for p in last_seen]
            raise TaskJobError(
                label_selector,
                f"Timed out waiting for pods (expected {expected}). Last seen: {names}",
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

    async def get_pod_logs(
        self, pod_name: str, container: Optional[str] = None, tail_lines: int = 500
    ) -> str:
        core_api, _ = self._require_clients()

        def _logs() -> str:
            return core_api.read_namespaced_pod_log(
                name=pod_name,
                namespace=self.namespace,
                container=container,
                tail_lines=tail_lines,
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

    @staticmethod
    def _is_pod_ready(pod: V1Pod) -> bool:
        if pod.status.phase != "Running":
            return False
        if not pod.status.container_statuses:
            return False
        return all(cs.ready for cs in pod.status.container_statuses)


__all__ = ["KubernetesClients", "PodMountCheckResult", "PodPathCheckResult", "VolcanoJobRunner"]
