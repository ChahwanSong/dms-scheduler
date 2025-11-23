# Development guide

## Environment

- Python 3.11+
- Redis reachable at `REDIS_WRITER_URL` and `REDIS_READER_URL` (defaults target the Kubernetes HAProxy endpoints).

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
```

## Running tests and linting

```bash
pytest
```

## Running locally

```bash
export DMS_OPERATOR_TOKEN=changeme
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## Design notes

- The scheduler is asynchronous throughout to support high concurrency.
- All business logic is isolated in `app/services` so route handlers remain thin.
- State is persisted in Redis using clear key prefixes (`dms:*`) to allow debugging with `redis-cli`.
