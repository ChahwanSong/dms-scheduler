# Architecture and internals

## Components
- **API layer (`src/dms_scheduler/api`)**: FastAPI routers for task operations (`/tasks`) and admin controls (`/admin`). Input validation relies on Pydantic models.
- **Service layer (`src/dms_scheduler/services`)**: Contains the `StateStore` (Redis persistence), `TaskExecutor` (async execution and state transitions), and `AdminService` (priority and blocking controls).
- **Core utilities (`src/dms_scheduler/core`)**: Environment-driven settings, Redis client abstraction for read/write hosts, and logging configuration suited for container stdout/stderr.
- **Models (`src/dms_scheduler/models`)**: Task payloads, status enums, and persisted task state with timestamps, logs, priority, and results.

## Redis schema
- Tasks: `task:{task_id}` → JSON document containing `task_id`, `service`, `user_id`, `parameters`, `status`, `logs`, `result`, `priority`, and timestamps.
- Blocking flags:
  - Global: `blocks:all` → `{ "blocked": true|false, "blocked_users": ["alice", ...] }`
  - Per-user: `blocks:user:{user_id}` → `{ "blocked": true|false }`

These keys allow state to be recovered after pod restarts because the scheduler reuses Redis for the source of truth.

## Execution flow
1. **Submit task** (`POST /tasks/task`):
   - Validates payload and checks global/user block flags.
   - Persists an `accepted` state and logs the action.
   - Launches an async worker (`TaskExecutor`) that transitions the task through `running` → `completed` (or `failed`) and writes `pod_status` / `launcher_output` into `result`.
2. **Cancel task** (`POST /tasks/cancel`):
   - Updates the task status to `cancelled` and records the log entry.
3. **Admin controls** (`/admin/*`):
   - Priority updates write to task state and append a log entry.
   - Blocking APIs toggle global or per-user flags that gate new submissions.

## Logging
Structured, stdout logging is configured at startup through `DMS_LOG_LEVEL` and `DMS_SERVICE_NAME`. This aligns with Kubernetes log aggregation.

## Extending
- Replace the placeholder `TaskExecutor._run_task` logic with real sync/copy work while keeping the status transitions intact.
- Use the `StateStore` helpers when adding new operations to ensure timestamps and logs remain consistent.
- Add new admin features by extending `AdminService` and wiring routes in `src/dms_scheduler/api/admin.py`.
