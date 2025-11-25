# DMS Scheduler

DMS Scheduler is a FastAPI-based backend for orchestrating Data Moving Service (DMS) tasks initiated by `dms-frontend`. The scheduler shares the same Redis keys as `dms-frontend`: the frontend creates and indexes tasks, and the scheduler updates those records (status, logs, results) as work progresses.

## Overview
- Receives task submissions from `dms-frontend` and updates task metadata (status, logs, parameters, results) in Redis.
- Executes work asynchronously (placeholder execution prints the payload) and updates task state as it runs.
- Supports task cancellation, priority changes, and administrative blocking of incoming requests.
- All APIs are exposed via FastAPI and designed for Kubernetes-friendly logging and configuration through environment variables.

## Prerequisites
- Python 3.11 or later.
- Redis reachable from the scheduler for read/write endpoints (or use the in-memory stub in tests).
- Optional: a virtual environment for isolating Python dependencies.

## Architecture
```
+-------------------+        +-------------------+
|   dms-frontend    | -----> |   dms-scheduler   |
+-------------------+        +-------------------+
          |                            |
          | Redis (state, logs, etc.)  |
          +----------------------------+
```

Core modules:
- `app/api`: FastAPI routers for task and admin operations.
- `app/services`: Stateless service layer for state persistence, task execution, and admin helpers.
- `app/core`: Settings, Redis client, shared task repository, and logging helpers.
- `app/models`: Pydantic models for task payloads, task state, and enums.

See `docs/architecture.md` for deeper details.

## Installation
Create a virtual environment (recommended) and install the project in editable mode:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Configuration
Environment variables (defaults target the HAProxy Redis endpoints):
- `DMS_REDIS_WRITE_URL`: write endpoint (default `redis://haproxy-redis.dms-redis.svc.cluster.local:6379/0`).
- `DMS_REDIS_READ_URL`: read endpoint (default `redis://haproxy-redis.dms-redis.svc.cluster.local:6380/0`).
- `DMS_OPERATOR_TOKEN`: token required for admin APIs (default `changeme`).
- `DMS_SERVICE_NAME`: service name for logging (default `dms-scheduler`).
- `DMS_LOG_LEVEL`: log level (default `INFO`).

## Running locally
Start the API server (defaults to `0.0.0.0:9000`):

```bash
python -m app.main --reload
```

Override the bind address or port when needed:

```bash
python -m app.main --host 127.0.0.1 --port 8000
```

## API reference
- `POST /tasks/task`: Submit a task. Body: `{ "task_id": "10", "service": "sync", "user_id": "alice", "parameters": {"src": "/home/gpu1", "dst": "/home/cpu1"} }`.
- `POST /tasks/cancel`: Cancel a task. Body: `{ "task_id": "10", "service": "sync", "user_id": "alice" }`.
- `POST /admin/priority`: Update priority (`high` or `low`). Requires `X-Operator-Token: <DMS_OPERATOR_TOKEN>`.
- `POST /admin/block` / `POST /admin/enable`: Block or enable all frontend requests. Requires operator token.
- `POST /admin/users/{user_id}/block` / `POST /admin/users/{user_id}/enable`: Block or enable a specific user. Requires operator token.
- `GET /healthz`: Liveness check.

Task lifecycle states are persisted in Redis and updated as the scheduler runs (pending → dispatching → running → completed/failed/cancelled). If a task ID has not been pre-registered by `dms-frontend`, the scheduler returns a 404 instead of implicitly creating the record.

## Usage examples
Practical examples (curl and Python) are available in the [`test`](test) directory:
- `api_usage.md` shows typical curl invocations.
- `temp_result_update.py` demonstrates updating `pod_status` and `launcher_output` fields in task results.

## Testing
```bash
pytest
```

The automated tests use an in-memory Redis stub to validate task submission, cancellation, blocking, and priority updates.
