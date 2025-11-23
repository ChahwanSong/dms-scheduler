# DMS Scheduler

DMS Scheduler is the priority-aware queueing layer of the Data Moving Service (DMS). It exposes a FastAPI microservice that accepts task requests from the frontend, keeps persistent state in Redis, and hands work to the submitter tier. The scheduler is built for Kubernetes and Redis deployments described in the accompanying architecture.

## Features

- **Strict priority queues**: three FIFO queues (`high`, `mid`, `low`) with deterministic dispatch order.
- **Redis-backed state**: task metadata, queue ordering, statistics, and control flags are stored in Redis for restart-safe operation.
- **Admin controls**: operator token–protected endpoints for priority changes and frontend request blocking/resuming.
- **Runtime statistics**: counters for queued, dispatched, and canceled tasks plus live queue sizes.
- **Local submitter**: a lightweight tool to pull the next task during local development.

## Architecture overview

```
+-------------+           +--------------------+           +----------------+
| dms-frontend| --POST--> |    dms-scheduler   | --pull--> | dms-submitter  |
+-------------+           +--------------------+           +----------------+
        ^                         |   ^                              |
        |                         |   |                              |
        |                     Redis KV Store (writer/read-only services)
        |                         |   |                              |
        |         read status ----+   +----- stats / control --------+
```

The scheduler hosts:

- **Task ingestion** from `dms-frontend`.
- **Strict priority queueing** with three FIFO queues.
- **Submitter handoff** via `/submitter/next` for the submitter to pop the next task.
- **State & metrics persistence** in Redis so the scheduler can restart without losing state.

Redis endpoints for the default Kubernetes deployment:

- **write**: `haproxy-redis.redis.svc.cluster.local:6379`
- **read**: `haproxy-redis.redis.svc.cluster.local:6380`

## Getting started

### Prerequisites

- Python 3.11+
- Access to Redis (defaults match the Kubernetes addresses above)

### Installation

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

### Running the API locally

```bash
export DMS_OPERATOR_TOKEN=changeme
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Useful environment variables

- `REDIS_WRITER_URL`: Redis URL for writes (default `redis://haproxy-redis.redis.svc.cluster.local:6379/0`).
- `REDIS_READER_URL`: Redis URL for reads (default `redis://haproxy-redis.redis.svc.cluster.local:6380/0`).
- `DMS_OPERATOR_TOKEN`: token required for admin endpoints.
- `LOG_LEVEL`: logging level (default `INFO`).

## API highlights

- `POST /tasks` – enqueue a task (default priority: `low`; `mid`/`high` require `X-DMS-Operator-Token`).
- `POST /tasks/cancel` – cancel a queued task.
- `GET /tasks/{task_id}` – task metadata plus queue position and overview.
- `GET /queues/overview` – aggregated queue sizes.
- `POST /submitter/next` – pop the next task for the submitter (priority-aware FIFO).
- `POST /admin/priority` – change a queued task's priority (requires `X-DMS-Operator-Token`).
- `POST /admin/block` / `POST /admin/resume` / `GET /admin/block` – toggle, resume, or read frontend block state (admin only).
- `GET /stats` – scheduler counters and live queue sizes.

Detailed request/response examples are available in [`docs/api.md`](docs/api.md).

## Local submitter tool

A helper script pulls the next task locally:

```bash
python tools/local_submitter.py --api-url http://localhost:8000
```

It prints the dispatched task payload or indicates when no work is pending.

## Development

- Code is type-hinted and organized into small modules under `app/`.
- Tests use `pytest`; run them with `pytest`.

## Project layout

```
app/
  api/            # FastAPI route handlers
  services/       # Business logic
  config.py       # Settings and operator token guard
  redis_client.py # Redis persistence and queue primitives
  main.py         # FastAPI application factory

Tools and docs:
- tools/local_submitter.py
- docs/architecture.md
- docs/api.md
- docs/development.md
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
