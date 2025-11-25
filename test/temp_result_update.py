"""Demonstrate updating pod_status and launcher_output in task state."""
import asyncio
import json

from dms_scheduler.models.schemas import TaskResult, TaskState, TaskStatus
from dms_scheduler.services.state_store import StateStore
from dms_scheduler.core.redis import RedisClient


class TempRedis(RedisClient):
    def __init__(self):
        super().__init__()
        self._data: dict[str, str] = {}

    async def connect(self):
        return None

    async def close(self):
        return None

    async def write_json(self, key: str, value):
        self._data[key] = json.dumps(value, default=str)

    async def read_json(self, key: str):
        raw = self._data.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def delete(self, key: str):
        self._data.pop(key, None)

    async def exists(self, key: str) -> bool:
        return key in self._data


async def main():
    store = StateStore(TempRedis())
    task = TaskState(
        task_id="demo",
        service="sync",
        user_id="alice",
        parameters={"src": "/tmp/src", "dst": "/tmp/dst"},
        status=TaskStatus.running,
    )
    await store.save_task(task)

    await store.set_result(
        "demo",
        TaskResult(pod_status="Running", launcher_output="launcher started"),
        message="Launcher output captured",
    )
    await store.set_result(
        "demo",
        TaskResult(pod_status="Succeeded", launcher_output="launcher finished"),
        message="Task finished",
    )
    final_state = await store.load_task("demo")
    print(final_state.model_dump())


if __name__ == "__main__":
    asyncio.run(main())
