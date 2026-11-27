# Distributed Job Scheduler

## Project overview and implementation blueprint

**Repository:** `distributed-job-scheduler`\
**Project type:** Backend infrastructure / distributed systems\
**Primary language:** Java 21\
**Framework:** Spring Boot\
**Messaging:** Apache Kafka\
**Database:** MySQL\
**Build:** Maven\
**Local infrastructure:** Docker Compose\
**Audience:** Developers who need reliable background task execution\
**Portfolio objective:** Demonstrate practical Java backend,
concurrency, messaging, scheduling, and reliability skills for fresher
placements.

------------------------------------------------------------------------

## 1. Executive summary

The Distributed Job Scheduler is a backend platform that accepts
background jobs from client applications, stores them durably, schedules
them for execution, dispatches them through Kafka, and processes them
using one or more worker processes.

A client should not need to implement its own scheduling loop, worker
pool, retry policy, execution history, and recovery logic for every
background task. It can submit a supported job to this platform and
query its status.

The central challenge is not CRUD. It is making asynchronous work
execute at the right time and handling the real failure cases that occur
when databases, message brokers, schedulers, and workers do not all
succeed together.

## 2. Problem statement

Applications commonly need work that should happen later or outside the
request-response cycle: - Send a reminder at a future time. - Generate a
daily report. - Process a large batch of records. - Run a recurring
maintenance task. - Retry a task after a temporary dependency failure.

A naive implementation using an in-memory timer or a single application
process can lose work on restart, block request threads, execute tasks
concurrently by mistake, or fail without a reliable recovery path.

This project provides durable job submission, scheduled dispatch,
asynchronous execution, controlled retries, and execution tracking.

## 3. Goals and non-goals

### Goals

1.  Accept and validate job submissions through REST.
2.  Persist job definitions and execution history.
3.  Execute one-time scheduled jobs.
4.  Dispatch due jobs through Kafka.
5.  Support multiple worker processes.
6.  Prevent competing schedulers from claiming the same job.
7.  Make processing safe against duplicate message delivery.
8.  Retry transient failures with a bounded policy.
9.  Make final failures inspectable.
10. Prove important behavior with automated tests and a reproducible
    local setup.

### Non-goals for the first version

-   Running arbitrary user-uploaded code or scripts.
-   Building a generic workflow/BPM platform.
-   Supporting every cron syntax or calendar rule immediately.
-   Integrating cloud queues or managed infrastructure.
-   Building a large admin dashboard before backend correctness.
-   Introducing Kubernetes or a large microservice estate.
-   Claiming exactly-once execution across all systems.

## 4. Actors

### Client application / developer

Submits jobs, receives a job identifier, checks status, lists jobs, and
cancels eligible jobs.

### Scheduler

Finds jobs whose scheduled time has arrived, safely claims them, and
dispatches execution messages.

### Worker

Consumes execution messages, selects the registered handler for the job
type, performs the task, and records the result.

### Operator / maintainer

Inspects failures, retries, logs, health, and execution history. A
separate UI is optional; APIs and logs are sufficient for the first
version.

## 5. Main use cases

  -----------------------------------------------------------------------
  Use case                            Expected behavior
  ----------------------------------- -----------------------------------
  Submit a job                        Validate input, persist it, return
                                      an ID

  Query a job                         Return current state and relevant
                                      metadata

  List jobs                           Return paginated jobs with optional
                                      filters

  Execute due job                     Dispatcher claims and publishes it

  Process job                         Worker invokes the registered
                                      handler

  Complete job                        Persist successful outcome

  Retry job                           Schedule a retry for an eligible
                                      transient failure

  Exhaust retries                     Mark job failed and preserve
                                      failure details

  Cancel job                          Cancel only if its current state
                                      allows safe cancellation

  Recover after restart               Find durable work that is due or
                                      abandoned and recover according to
                                      defined rules
  -----------------------------------------------------------------------

## 6. Functional requirements

### FR-01: Job submission

The API accepts a job name, supported job type, JSON payload, scheduled
timestamp, and retry configuration. It validates required fields and
returns a stable job ID.

### FR-02: Durable storage

A successfully accepted job is stored in MySQL. Restarting the
application must not erase pending jobs.

### FR-03: Job querying

A client can retrieve a job by ID and list jobs using pagination and
filters such as state, type, or time range.

### FR-04: Due-job scheduling

The dispatcher identifies jobs whose scheduled time is due and that are
eligible for dispatch.

### FR-05: Safe claiming

A job must be claimed through an atomic database operation or equivalent
transaction/locking strategy so that competing dispatcher instances
cannot both claim it.

### FR-06: Asynchronous dispatch

A claimed job is represented by a dedicated event/message and sent to
Kafka. JPA entities must not be used as Kafka event contracts.

### FR-07: Worker execution

A worker consumes the message and invokes a registered handler for the
job type. The first release supports a small allowlist of job types.

### FR-08: Execution history

Each attempt records relevant timestamps, outcome, attempt number, and
failure information where applicable.

### FR-09: Retry policy

Retryable failures are retried up to the configured maximum using a
defined backoff policy. Permanent failures are not retried indefinitely.

### FR-10: Idempotent processing

Repeated delivery of the same execution message must not repeat the
protected business operation. A stable execution identifier and
persisted processing state are required.

### FR-11: Cancellation

A client may cancel a job only in states where cancellation can be
enforced safely. Cancellation after a worker has begun execution
requires an explicit policy and is not assumed to stop arbitrary running
code.

### FR-12: Operational visibility

The application exposes useful logs and health information. Metrics and
dashboards may be added after the core flow works.

## 7. Non-functional requirements

-   **Durability:** Accepted jobs survive application restarts.
-   **Consistency:** Job state changes follow defined, validated
    transitions.
-   **Concurrency safety:** Competing dispatchers cannot claim the same
    job.
-   **Resilience:** Transient failures have bounded retries; permanent
    failures become visible.
-   **Scalability:** Worker count can be increased to process
    independent jobs concurrently.
-   **Maintainability:** Clear package/module boundaries and separate
    API DTOs, persistence models, and event contracts.
-   **Testability:** Scheduling, claiming, retries, idempotency, and
    recovery are covered by automated tests.
-   **Security:** Only supported job types execute; arbitrary code
    execution is excluded.
-   **Observability:** Logs should allow an operator to trace a job
    across dispatch and worker execution.

## 8. Proposed architecture

``` text
             Client Application
                    |
                    | REST
                    v
       +---------------------------+
       | Scheduler API             |
       | - validate request        |
       | - submit/query/cancel     |
       +-------------+-------------+
                     |
                     v
       +---------------------------+
       | MySQL                     |
       | - jobs                    |
       | - execution_attempts      |
       | - idempotency/inbox       |
       | - outbox (later phase)    |
       +-------------+-------------+
                     ^
                     | safe claim / state update
                     |
       +-------------+-------------+
       | Scheduler / Dispatcher    |
       | - find due jobs           |
       | - claim atomically        |
       | - publish execution event |
       +-------------+-------------+
                     |
                     v
       +---------------------------+
       | Kafka                     |
       | job-execution topic       |
       | retry topic (later)       |
       | dead-letter topic (later) |
       +-------------+-------------+
                     |
              +------+------+
              |             |
              v             v
       +-------------+ +-------------+
       | Worker A    | | Worker B    |
       | handler     | | handler     |
       +------+------+ +------+------+
              |               |
              +-------+-------+
                      |
                      v
               MySQL outcome
```

### Architectural approach

Start with a modular Spring Boot application and a worker process that
can be run separately. Keep boundaries explicit in code. Split into
independently deployable services only if a concrete scaling,
deployment, or learning requirement justifies it.

The diagram represents logical responsibilities. It does not require
every box to become a separate microservice on day one.

## 9. Job lifecycle and state transitions

### Initial states

  -----------------------------------------------------------------------
  State                               Meaning
  ----------------------------------- -----------------------------------
  `SCHEDULED`                         Persisted and waiting for its
                                      scheduled time

  `QUEUED`                            Claimed/selected for dispatch and
                                      published or awaiting confirmed
                                      dispatch according to the chosen
                                      implementation

  `RUNNING`                           A worker has claimed the execution

  `RETRY_PENDING`                     A retry is scheduled after an
                                      eligible failure

  `COMPLETED`                         Execution succeeded

  `FAILED`                            Permanent failure or retry limit
                                      exhausted

  `CANCELLED`                         Cancelled before execution under
                                      the cancellation rules
  -----------------------------------------------------------------------

### Illustrative transitions

``` text
SCHEDULED -> QUEUED -> RUNNING -> COMPLETED
                              |
                              +-> RETRY_PENDING -> QUEUED
                              |
                              +-> FAILED

SCHEDULED -> CANCELLED
QUEUED -> CANCELLED   (only if not yet claimed by a worker)
```

The exact semantics of `QUEUED`, publication confirmation, worker
claiming, and cancellation must be made precise during design. Do not
allow API clients to arbitrarily set internal states.

### Important invariants

-   Only eligible jobs can be claimed.
-   A job cannot be completed before it has entered execution.
-   Attempt numbers increase consistently.
-   Retry count cannot exceed the configured limit.
-   Duplicate message delivery cannot repeat the protected operation.
-   Cancellation cannot silently report success after execution has
    already started.

## 10. Data model direction

The exact schema will be finalized in Phase 0/Phase 1. A likely starting
point:

### `jobs`

Potential fields: - `id` --- UUID or another stable identifier. -
`job_name` - `job_type` - `payload` --- JSON. - `status` -
`scheduled_at` --- stored using a consistent UTC convention. -
`max_retries` - `attempt_count` - `next_attempt_at` - `created_at` -
`updated_at` - `last_error` - `version` --- if optimistic locking is
selected.

### `execution_attempts`

Potential fields: - `id` - `job_id` - `attempt_number` - `worker_id` -
`status` - `started_at` - `finished_at` - `error_message` -
`result_summary`

### Later reliability tables

-   `processed_messages` / inbox table for deduplication.
-   `outbox_events` for reliable event publication.
-   Optional worker lease/heartbeat data if needed for recovery.

Use Flyway migrations. Do not rely indefinitely on Hibernate automatic
schema creation. Choose appropriate indexes after query patterns are
known, especially for due-job lookup and status filtering.

## 11. API design direction

These are proposed endpoints; finalize request/response schemas and
status codes during design.

  ---------------------------------------------------------------------------
  Method                  Endpoint                    Purpose
  ----------------------- --------------------------- -----------------------
  `POST`                  `/api/jobs`                 Submit a job

  `GET`                   `/api/jobs/{id}`            Get job details/status

  `GET`                   `/api/jobs`                 List jobs with
                                                      pagination/filters

  `POST` or `DELETE`      `/api/jobs/{id}/cancel`     Request cancellation

  `GET`                   `/api/jobs/{id}/attempts`   View execution history

  Optional `POST`         `/api/jobs/{id}/retry`      Manually retry an
                                                      eligible failed job
  ---------------------------------------------------------------------------

### Illustrative submission request

``` json
{
  "jobName": "GenerateDailyReport",
  "jobType": "REPORT",
  "payload": {
    "reportType": "SALES"
  },
  "scheduledAt": "2026-11-26T00:00:00Z",
  "maxRetries": 3
}
```

The timestamp above is illustrative. The API must explicitly define
whether it accepts offset-aware timestamps and how it stores/compares
time. Prefer UTC internally.

Use request/response DTOs with validation. Do not expose JPA entities
directly.

## 12. Event contract direction

Kafka messages should be dedicated versioned DTOs, not serialized
database entities.

An execution event should carry enough information to identify and
process the intended attempt, for example: - `eventId` - `eventType` -
`schemaVersion` - `jobId` - `executionId` or attempt identifier -
`jobType` - `payload` - `occurredAt`

The final schema and partition key should be chosen based on ordering
and throughput requirements. Do not assume one key strategy is correct
without considering the required ordering semantics.

## 13. Reliability design challenges

### 13.1 Database/Kafka dual-write problem

Updating a job row and publishing a Kafka message are two separate
operations. One may succeed while the other fails. First build and test
the basic flow; then consider a transactional outbox so the state change
and event record commit together in MySQL, with a separate publisher
forwarding outbox records to Kafka.

### 13.2 Duplicate delivery and idempotency

Kafka consumers can receive a message more than once. Persist processed
execution/message identifiers and make the business operation safe to
repeat. Explain exactly which operation is protected and what happens if
a worker crashes between performing the operation and recording
completion.

### 13.3 Concurrent dispatchers

Use an atomic conditional update or appropriate row-locking approach to
claim due jobs. A read-then-update sequence without protection is unsafe
when multiple dispatchers run concurrently.

### 13.4 Worker crash and recovery

Define a lease/heartbeat or timeout policy for jobs marked `RUNNING`. A
recovery process can identify abandoned attempts and decide whether to
retry. Do not automatically re-run a job without considering whether its
handler may have partially completed an external side effect.

### 13.5 Retry policy

Use bounded retries, backoff, and error classification. Permanent
validation/unsupported-type errors should fail without endless retries.
Preserve attempt history.

### 13.6 Dead-letter handling

After retry exhaustion or non-retryable message failure, make the event
inspectable in a dead-letter path. Document safe replay rather than
blindly replaying messages.

### 13.7 Exactly-once claims

Do not claim end-to-end exactly-once business execution because Kafka
offers exactly-once features. Database writes, external side effects,
consumer redelivery, and crash windows must be considered. Prefer clear
at-least-once delivery plus idempotent processing unless a narrower
guarantee is proven.

## 14. Testing strategy

Tests should prove behavior and invariants.

### API and domain tests

-   Valid job submission persists a `SCHEDULED` job.
-   Invalid job type or malformed payload is rejected.
-   Query returns correct state.
-   Pagination and filters behave correctly.
-   Cancellation is rejected when the job is already running/completed.

### Scheduler tests

-   Future jobs are not dispatched early.
-   Due jobs become eligible.
-   A job is not claimed twice by competing dispatchers.
-   Restart does not lose pending jobs.

### Worker and Kafka tests

-   Valid event invokes the correct handler.
-   Successful handler marks the attempt/job complete.
-   Duplicate event does not repeat the protected operation.
-   Transient failure schedules a retry.
-   Permanent failure does not retry.
-   Retry exhaustion marks the job failed.
-   Multiple workers can process independent jobs.

### Recovery tests

-   Simulate worker interruption after claim.
-   Verify abandoned execution recovery follows the defined policy.
-   Test publication failure and outbox recovery once the outbox exists.

Use unit tests first, then integration tests. Testcontainers may be
introduced when useful for realistic MySQL/Kafka tests.

## 15. Development phases

  -------------------------------------------------------------------------
  Phase                   Work                      Exit criteria
  ----------------------- ------------------------- -----------------------
  0 --- Design            Requirements, actors,     Decisions documented
                          state machine,            
                          architecture, repo        

  1 --- Foundation        Spring Boot, MySQL,       API and persistence
                          Flyway, models, REST APIs tests pass

  2 --- Scheduler         Due-job query and safe    Due-time and
                          claiming                  concurrency tests pass

  3 --- Kafka/workers     Event DTOs, Kafka         End-to-end job executes
                          producer/consumer,        
                          handlers                  

  4 --- Reliability       Idempotency, retries,     Failure-path tests pass
                          DLT, outbox/recovery      

  5 --- Quality           Observability,            Behavior is
                          cancellation,             demonstrable
                          integration/concurrency   
                          tests                     

  6 --- Portfolio         Swagger, README,          Another developer can
                          diagrams, demo, setup     run and understand it
                          guide                     
  -------------------------------------------------------------------------

Estimated planning range: roughly 3--4 weeks at 2--3 focused hours per
day for the intermediate project, with debugging and learning
potentially extending the schedule. This is an estimate, not a deadline
guarantee.

## 16. Repository structure direction

Proposed structure:

``` text
distributed-job-scheduler/
├── scheduler-api/
│   └── src/
├── scheduler-core/
│   └── src/
├── scheduler-worker/
│   └── src/
├── infrastructure/
│   └── docker-compose.yml
├── docs/
│   ├── PROJECT_CONTEXT.md
│   ├── PROJECT_OVERVIEW.md
│   ├── requirements.md
│   ├── architecture.md
│   └── job-lifecycle.md
├── postman/
├── README.md
└── .gitignore
```

This is a logical proposal. Decide whether to use a single Maven module
with clear packages, a Maven multi-module build, or a small number of
Spring Boot applications before generating the structure. Do not create
empty modules just to match the diagram.

## 17. Placement and portfolio presentation

A concise resume description, once the corresponding features are
implemented and tested:

**Distributed Job Scheduler \| Java 21, Spring Boot, Kafka, MySQL,
Docker** - Built a backend platform for durable scheduling and
asynchronous execution of background jobs using Kafka and worker
processes. - Implemented database-backed job claiming, execution
tracking, and bounded retry handling. - Added idempotent message
processing and concurrency tests to protect job execution against
duplicate delivery and competing dispatchers.

Only retain bullets that are genuinely implemented and verified. Add
measurable throughput/latency figures only after running reproducible
benchmarks; never invent performance numbers.

### Interview topics this project can support

-   Thread pools and concurrency.
-   Scheduling and time handling.
-   Kafka partitions, consumer groups, offsets, and redelivery.
-   Database transactions and row locking.
-   Idempotency and at-least-once delivery.
-   Retry/backoff and dead-letter queues.
-   Transactional outbox and dual writes.
-   Worker leases and crash recovery.
-   Modular monolith versus microservices.
-   Integration and concurrency testing.

## 18. Immediate next steps

1.  Create the GitHub repository `distributed-job-scheduler`.
2.  Clone it under `D:\IdeaProjects\LearningBoot\Projects\`.
3.  Add these project documents under `docs/`.
4.  Confirm the actual repository URL and local folder.
5.  Complete `requirements.md`, `architecture.md`, and
    `job-lifecycle.md`.
6.  Decide the initial Maven structure and Docker Compose topology.
7.  Only then create the Spring Boot application.

## 19. Project decision log

  -----------------------------------------------------------------------------
  Decision                Current position              Status
  ----------------------- ----------------------------- -----------------------
  Project direction       Distributed Job Scheduler     Selected

  Main goal               Placement-focused Java        Selected
                          backend portfolio             

  Difficulty              Intermediate                  Selected

  Repository name         `distributed-job-scheduler`   Selected

  Language                Java 21                       Selected

  Framework               Spring Boot                   Selected

  Database                MySQL                         Selected

  Messaging               Kafka                         Selected

  Build                   Maven                         Selected

  Local infrastructure    Docker Compose                Selected

  Architecture            Modular backend + separately  Proposed; finalize in
                          runnable workers              Phase 0

  Job types               Small predefined allowlist    Proposed

  Recurring jobs          After one-time scheduling     Planned
                          works                         

  Redis                   Later only if justified       Deferred

  Frontend                Optional after backend        Deferred
                          correctness                   

  Authentication          Later if justified            Deferred
  -----------------------------------------------------------------------------

## 20. Important project status note

This document describes the intended design, not a claim that the
implementation already exists. At creation time, repository creation,
cloning, and the initial Git commit have not been verified. Do not
assume any source code, database schema, Docker setup, Kafka topic,
endpoint, or test exists until the user confirms it or the repository is
inspected.
