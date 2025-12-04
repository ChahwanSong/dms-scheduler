# Architecture and internals

## Components
- **API layer (`app/api`)**: FastAPI routers for task operations and task controls (all under `/tasks`). Input validation relies on Pydantic models.
- **Service layer (`app/services`)**: Contains the `StateStore` (Redis persistence), `TaskExecutor` (async execution and state transitions), and `AdminService` (priority and blocking controls).
- **Core utilities (`app/core`)**: Environment-driven settings, Redis client abstraction for read/write hosts, a shared Redis task repository, and logging configuration suited for container stdout/stderr.
- **Models (`app/models`)**: Task payloads, status enums, and persisted task state with timestamps, logs, priority, and results.

## Redis schema
- Tasks: `task:{task_id}` → JSON document containing `task_id`, `service`, `user_id`, `parameters`, `status`, `logs`, `result`, `priority`, `active_jobs`, and timestamps.
- Indices maintained by the shared repository for querying: `index:tasks` (all task IDs), `index:service:{service}` (IDs per service), `index:service:{service}:users` (users per service), and `index:service:{service}:user:{user_id}` (IDs per service/user). TTLs mirror the task TTL to keep indices in sync.
- Metadata used for cleanup: `task:{task_id}:metadata` contains `service` and `user_id` and follows the same TTL.
- Blocking flags:
  - Global: `blocks:all` → `{ "blocked": true|false, "blocked_users": ["alice", ...] }`
  - Per-user: `blocks:user:{user_id}` → `{ "blocked": true|false }`

The scheduler does **not** subscribe to Redis key expiration events; `dms-frontend` owns expiration handling and task creation. If a task ID is missing in Redis, scheduler APIs respond with a 404 instead of recreating it.

## Execution flow
1. **Submit task** (`POST /tasks/task`):
   - Validates payload and checks global/user block flags.
   - Sets the task status to `dispatching` and logs the action; missing task IDs return 404 because records must already exist in Redis from `dms-frontend`.
   - Launches an async worker (`TaskExecutor`) that transitions the task through `running` → `completed` (or `failed`) and writes `pod_status` / `launcher_output` into `result`.
   - Each handler registers created Kubernetes job names under `active_jobs` in Redis so cancellation survives scheduler restarts.
2. **Cancel task** (`POST /tasks/cancel`):
   - Verifies the request matches the stored task `service` and `user_id`; mismatches return a 403.
   - Marks the task `cancel_requested`, deletes any tracked Kubernetes jobs, clears `active_jobs`, and appends log entries for transparency.
   - If the running handler surfaces job failures after cancellation, the executor translates them into task cancellation rather than a regular failure.
3. **Task controls** (`/tasks/*`):
   - Priority updates write to task state and append a log entry.
   - Blocking APIs toggle global or per-user flags that gate new submissions.

## Logging
Structured, stdout logging is configured at startup through `DMS_LOG_LEVEL` and `DMS_SERVICE_NAME`. This aligns with Kubernetes log aggregation.

## Extending
- Replace the placeholder `TaskExecutor._run_task` logic with real sync/copy work while keeping the status transitions intact.
- Use the `StateStore` helpers when adding new operations to ensure timestamps and logs remain consistent.
- Add new control features by extending `AdminService` and wiring routes in `app/api/admin.py`.

## Sync handler directory validation
- Allowed prefixes are defined in `app/services/constants.py` (`ALLOWED_DIRECTORIES`), and requests must place `src`/`dst` paths under one of those prefixes.
- If validation fails, the scheduler raises `TaskInvalidDirectoryError` with a message of the form `Invalid path to service 'sync': <path>`, which surfaces as a 400/404 response in the API layer depending on context.
- Tests covering these messages live in `tests/test_sync_handler.py` to keep the behavior stable when adding new storage mounts or adjusting error phrasing.

## Running the API server
Use the built-in launcher to start Uvicorn with sensible defaults:

```bash
python -m app.main  # binds to 0.0.0.0:9000
```

Override the interface or port as needed:

```bash
python -m app.main --host 127.0.0.1 --port 8000
```
