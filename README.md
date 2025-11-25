# DMS Scheduler

DMS Scheduler is the FastAPI-based backend responsible for executing and managing Data Moving Service (DMS) tasks initiated by the `dms-frontend`. The scheduler persists all state in Redis so Kubernetes restarts or pod rescheduling do not lose any task metadata.

## Overview
- Receives task submissions from `dms-frontend` and persists task metadata (status, logs, parameters, results) in Redis.
- Executes work asynchronously (placeholder execution prints the payload) and updates task state as it runs.
- Supports task cancellation, priority changes, and administrative blocking of incoming requests.
- All APIs are exposed via FastAPI and designed for Kubernetes-friendly logging and configuration through environment variables.

## Architecture
```
+-------------------+        +-------------------+
|   dms-frontend    | -----> |   dms-scheduler    |
+-------------------+        +-------------------+
          |                            |
          | Redis (state, logs, etc.)  |
          +----------------------------+
```

Core modules:
- `src/dms_scheduler/api`: FastAPI routers for task and admin operations.
- `src/dms_scheduler/services`: Stateless service layer for state persistence, task execution, and admin helpers.
- `src/dms_scheduler/core`: Settings, Redis client, and logging helpers.
- `src/dms_scheduler/models`: Pydantic models for task payloads, task state, and enums.

See `docs/architecture.md` for deeper details.

## Configuration
Environment variables (defaults target the HAProxy Redis endpoints):
- `DMS_REDIS_WRITE_HOST` / `DMS_REDIS_WRITE_PORT`: write endpoint (default `haproxy-redis.redis.svc.cluster.local:6379`).
- `DMS_REDIS_READ_HOST` / `DMS_REDIS_READ_PORT`: read endpoint (default `haproxy-redis.redis.svc.cluster.local:6380`).
- `DMS_OPERATOR_TOKEN`: token required for admin APIs (priority and blocking controls, default `changeme`).
- `DMS_SERVICE_NAME`: service name for logging (default `dms-scheduler`).
- `DMS_LOG_LEVEL`: log level (default `INFO`).

## Running locally
Install dependencies (Python 3.11):
```bash
pip install -e .[dev]
```

Start the API server:
```bash
uvicorn dms_scheduler.main:app --reload
```

## API summary
- `POST /tasks/task`: Submit a task. Body: `{ "task_id": "10", "service": "sync", "user_id": "alice", "parameters": {"src": "/home/gpu1", "dst": "/home/cpu1"} }`.
- `POST /tasks/cancel`: Cancel a task. Body: `{ "task_id": "10", "service": "sync", "user_id": "alice" }`.
- `POST /admin/priority`: Update priority (`high` or `low`). Requires `Authorization: Bearer <DMS_OPERATOR_TOKEN>`.
- `POST /admin/block` / `POST /admin/enable`: Block or enable all frontend requests. Requires operator token.
- `POST /admin/users/{user_id}/block` / `POST /admin/users/{user_id}/enable`: Block or enable a specific user. Requires operator token.
- `GET /healthz`: Liveness check.

Task lifecycle states are persisted in Redis and updated as the scheduler runs (accepted → running → completed/failed/cancelled).

## Examples
Practical examples (curl and Python) are available in the [`test`](test) directory:
- `api_usage.md` shows typical curl invocations.
- `temp_result_update.py` demonstrates updating `pod_status` and `launcher_output` fields in task results.

## Testing
```bash
pytest
```

The automated tests use an in-memory Redis stub to validate task submission, cancellation, blocking, and priority updates.
