# Requirements: Distributed Job Scheduler

Status: Day 1 decisions CONFIRMED. Items marked [DECIDED] were confirmed by the project owner.

## 1. MVP scope

In scope for v1:
- Submit one-time scheduled jobs through REST
- Durable storage in MySQL
- Query one job, list jobs (pagination and filters)
- Due-job dispatch through Kafka, executed by worker processes
- Execution history per attempt
- Bounded retries, idempotent processing, cancellation rules
- Basic logs and a health endpoint

Out of scope for v1: recurring/cron jobs, arbitrary user code, frontend, Redis,
authentication, cloud services, Kubernetes, exactly-once business claims.

## 2. Supported job types (allowlist)

| Type | Purpose | v1 handler behavior |
|---|---|---|
| `REPORT` | Generate a report | Simulated: builds a summary and writes it to the attempt's `result_summary` |
| `EMAIL` | Send a reminder | Simulated: logs "email sent" (no real SMTP in v1) |
| `DATA_PROCESSING` | Batch work | Simulated: processes N records, can be told to fail for testing |

**[DECIDED]** Only `REPORT` is implemented first (Day 18). The others come later.

Any other type is rejected with HTTP 400.
## 3. Actors

- Client application / developer: submits, queries, lists, cancels
- Scheduler (dispatcher): claims due jobs and publishes events
- Worker: consumes events and runs handlers
- Operator: inspects failures, logs, health

## 4. User stories

1. As a client, I submit a job with a type, payload, time and retry limit, and get back a job ID.
2. As a client, I fetch a job by ID and see its current state.
3. As a client, I list jobs filtered by state, type or time range, with pagination.
4. As a client, I cancel a job that has not started running.
5. As a client, I view the execution attempts of a job.
6. As the system, I run a job at or after its scheduled time, never before.
7. As the system, I run each job once even if two dispatchers or duplicate messages occur.
8. As an operator, I see why a job failed.

## 5. API behavior (provisional)
**Cancellation [DECIDED]:** cancelling a job that is running, completed, failed or already cancelled returns 409.

| Endpoint | Success | Errors |
|---|---|---|
| `POST /api/jobs` | 201 + job ID | 400 validation / unsupported type |
| `GET /api/jobs/{id}` | 200 | 404 not found |
| `GET /api/jobs?status=&type=&from=&to=&page=&size=` | 200 (paged) | 400 bad filter |
| `POST /api/jobs/{id}/cancel` | 200 | 404; 409 if already running/finished |
| `GET /api/jobs/{id}/attempts` | 200 | 404 |

<<<<<<< HEAD
**Time convention [decided]:** `scheduledAt` must be an ISO-8601 timestamp WITH an offset
(e.g. `2026-11-26T00:00:00Z` or `...+05:30`). Timestamps without an offset are rejected.
Stored in UTC.

**Validation rules [decided]:**
=======
**Time convention [DECIDED]:** `scheduledAt` must be an ISO-8601 timestamp WITH an offset
(e.g. `2026-11-26T00:00:00Z` or `...+05:30`). Timestamps without an offset are rejected.
Stored in UTC.

**Validation rules [DECIDED]:**
>>>>>>> 4b75561 (docs: confirm Day 1 MVP requirements)
- `jobName`: required, 1-100 chars
- `jobType`: required, must be in the allowlist
- `payload`: required, JSON object
- `scheduledAt`: required, not in the past (small tolerance, e.g. 5 seconds)
- `maxRetries`: optional, 0-10, default 3

**Error response shape:** `{ "timestamp", "status", "error", "message", "details": [] }`

## 6. Job states (details finalized on Day 2)

`SCHEDULED, QUEUED, RUNNING, RETRY_PENDING, COMPLETED, FAILED, CANCELLED`.
Clients can never set the state directly.

## 7. Non-functional requirements

- Durability: accepted jobs survive restarts
- Consistency: only validated state transitions
- Concurrency safety: two dispatchers cannot claim the same job
- Resilience: bounded retries, visible permanent failures
- Delivery guarantee: at-least-once delivery plus idempotent processing
- Security: only allowlisted handlers run
- Observability: a job can be traced by ID in logs
- Testability: scheduler, claim, retry, idempotency and recovery covered by tests

## 8. Open decisions for Day 2

- Exact meaning of `QUEUED` (before or after Kafka publish confirmation)
- Cancellation policy for `QUEUED`
- Job ID type (UUID vs numeric)
- Single Maven module vs multi-module
