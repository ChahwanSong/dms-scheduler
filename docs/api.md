# API Reference

All endpoints are served by the FastAPI app (`app.main:app`). The base path examples assume `http://localhost:8000`.

## Task lifecycle

### Submit task

`POST /tasks`

Body:
```json
{
  "task_id": "10",
  "service": "sync",
  "user_id": "alice",
  "parameters": {"src": "/home/gpu1", "dst": "/home/cpu1"},
  "priority": "low"
}
```

- Default priority is `low` when omitted.
- Submitting with `priority` of `mid` or `high` requires `X-DMS-Operator-Token`.
- Returns the persisted task record.
- If the scheduler is blocking frontend traffic, responds with `503`.

### Cancel task

`POST /tasks/cancel`

Body:
```json
{"task_id": "10", "service": "sync", "user_id": "alice"}
```

- Cancels queued tasks and updates status; idempotent for already-canceled tasks.
- Returns `404` if the task does not exist.

### Task status

`GET /tasks/{task_id}`

- Returns task metadata, queue position (priority + index), and queue overview.
- Returns `404` for unknown tasks.

### Queue overview

`GET /queues/overview`

- Returns counts for `high`, `mid`, `low`, and overall total.

### Submitter pop

`POST /submitter/next`

- Pops the next task using strict priority order.
- Response `204` if no work is available.

## Admin endpoints

All admin routes require the `X-DMS-Operator-Token` header to match `DMS_OPERATOR_TOKEN`.

### Change priority

`POST /admin/priority`

Body:
```json
{"task_id": "10", "priority": "high"}
```

- Only works for tasks with status `queued`.

### Toggle frontend block

`POST /admin/block`

Body:
```json
{"blocked": true, "reason": "maintenance"}
```

`GET /admin/block` reads the current block state.

### Resume frontend traffic

`POST /admin/resume`

Body:
```json
{"reason": "maintenance complete"}
```

- Clears the block flag and records an optional reason.

## Statistics

`GET /stats`

- Returns counters (`queued`, `dispatched`, `canceled`), last update time, and queue overview.
