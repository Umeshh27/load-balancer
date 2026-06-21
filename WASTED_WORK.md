# Wasted Work Analysis

## Problem Description

When a gRPC client sets a deadline (e.g., 2 seconds) but the server's downstream
dependency (Enrichment Service) takes longer (e.g., 3 seconds), the client times out
and abandons the request. However, **without deadline propagation**, the server
continues processing the request, wasting resources and writing stale data to the database.

## Test Configuration

| Parameter                  | Value        |
| -------------------------- | ------------ |
| Client Deadline            | 2 seconds    |
| Enrichment Service Latency | 3 seconds    |
| Deadline Propagation       | **DISABLED** |

## Observed Behavior

### Timeline

1. **T+0.0s**: Client sends `ProcessJob` request with 2-second deadline
2. **T+0.0s**: Worker receives request, forwards to Enrichment Service
3. **T+2.0s**: Client deadline expires → Client receives `DEADLINE_EXCEEDED`
4. **T+3.0s**: Enrichment Service responds to Worker (client already gone)
5. **T+3.1s**: Worker writes result to PostgreSQL database ← **WASTED WORK**

### Evidence: Worker Logs

```
Worker received ProcessJob: job_id=wasted-abc12345
Deadline propagation OFF. Processing without timeout forwarding.
Enrichment completed for job_id=wasted-abc12345. Writing to DB...
DB write completed for job_id=wasted-abc12345 (client may have already timed out!)
```

### Evidence: Database Query

```sql
SELECT job_id, worker_id, created_at FROM results WHERE job_id = 'wasted-abc12345';

-- Result:
-- job_id            | worker_id | created_at
-- wasted-abc12345   | worker-1  | 2024-01-01 12:00:03.100  ← Written 1.1s AFTER client timeout
```

## Impact

- **Resource Waste**: CPU, memory, network bandwidth consumed for results nobody will read
- **Database Bloat**: Stale rows accumulate in the database
- **Cascading Load**: Under high load, wasted work amplifies resource exhaustion
- **False Metrics**: Completed job counts don't reflect actual successful deliveries

## Fix

See Phase 3: Deadline Propagation. The worker must:

1. Extract remaining time from the incoming gRPC context
2. Pass it as timeout to the Enrichment Service call
3. Pass it as timeout to database queries
4. Handle `CancelledError` gracefully and roll back transactions
