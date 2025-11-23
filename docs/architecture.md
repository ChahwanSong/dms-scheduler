# Architecture

DMS Scheduler is a FastAPI application that persists all state in Redis so that the service can restart without losing queue order or task metadata.

## Components

- **API layer (`app/api`)**: HTTP endpoints for frontend, submitter, and admin actions.
- **Service layer (`app/services`)**: business rules for queueing, canceling, priority changes, and blocking.
- **Persistence (`app/redis_client.py`)**: Redis-backed queue operations and stats recording.
- **Configuration (`app/config.py`)**: environment-driven settings and operator token enforcement.

## Data model

- **Task record**: JSON stored at `dms:task:{task_id}` containing metadata, priority, and status.
- **Queues**: Redis lists per priority (`dms:queue:high|mid|low`) to guarantee FIFO order.
- **Task index**: set of all known task IDs (`dms:task:index`) for quick listing.
- **Stats**: Redis hash `dms:stats` with counters for `queued`, `dispatched`, `canceled`, and `last_updated` timestamp.
- **Block flag**: JSON blob at `dms:block:frontend` that controls whether new frontend requests are accepted.

## Queueing behavior

1. Tasks arrive via `POST /tasks` and are persisted to Redis with status `queued`.
2. The task ID is appended to the FIFO list for its priority (default `low`).
3. Submitters call `POST /submitter/next` to pop from `high` → `mid` → `low` order. The task status transitions to `dispatched`.
4. Users and admins can query `/tasks/{task_id}` to retrieve metadata, current status, and position across queues.

## Resiliency

All runtime state is in Redis. When the scheduler restarts, it rebuilds its view from Redis without special bootstrapping. Stats and block flags are also persisted.

## Security and control

- **Operator token**: Admin-only routes require the `X-DMS-Operator-Token` header to match `DMS_OPERATOR_TOKEN`.
- **Blocking**: `/admin/block` flips the block flag to temporarily reject new frontend task submissions.

## Logging

The service uses Python's standard logging. The `LOG_LEVEL` environment variable controls verbosity. In Kubernetes, logs are emitted to stdout/stderr for aggregation.
