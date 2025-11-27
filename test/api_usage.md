# API usage examples

## Submit a task
```bash
curl -X POST http://localhost:8000/tasks/task \
  -H "Content-Type: application/json" \
  -d '{"task_id": "10", "service": "sync", "user_id": "alice", "parameters": {"src": "/home/gpu1", "dst": "/home/cpu1"}}'
```

## Cancel a task
```bash
curl -X POST http://localhost:8000/tasks/cancel \
  -H "Content-Type: application/json" \
  -d '{"task_id": "10", "service": "sync", "user_id": "alice"}'
```

## Update task priority
```bash
curl -X POST http://localhost:8000/tasks/priority \
  -H "Content-Type: application/json" \
  -d '{"task_id": "10", "priority": "high"}'
```

## Block or enable all frontend requests
```bash
curl -X POST http://localhost:8000/tasks/block
curl -X POST http://localhost:8000/tasks/enable
```

## Block or enable a specific user
```bash
curl -X POST http://localhost:8000/tasks/users/alice/block
curl -X POST http://localhost:8000/tasks/users/alice/enable
```
