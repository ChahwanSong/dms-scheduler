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

## Update task priority (admin only)
```bash
curl -X POST http://localhost:8000/admin/priority \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer ${DMS_OPERATOR_TOKEN}" \
  -d '{"task_id": "10", "priority": "high"}'
```

## Block or enable all frontend requests (admin only)
```bash
curl -X POST http://localhost:8000/admin/block -H "Authorization: Bearer ${DMS_OPERATOR_TOKEN}"
curl -X POST http://localhost:8000/admin/enable -H "Authorization: Bearer ${DMS_OPERATOR_TOKEN}"
```

## Block or enable a specific user (admin only)
```bash
curl -X POST http://localhost:8000/admin/users/alice/block -H "Authorization: Bearer ${DMS_OPERATOR_TOKEN}"
curl -X POST http://localhost:8000/admin/users/alice/enable -H "Authorization: Bearer ${DMS_OPERATOR_TOKEN}"
```
