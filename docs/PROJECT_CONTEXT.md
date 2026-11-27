# Distributed Job Scheduler --- Project Context

**Purpose:** This file is the continuity/context guide for future
ChatGPT sessions working on this project. Treat it as the project source
of truth and update it whenever an architectural decision,
implementation detail, or verified behavior changes.

## 1. Project identity

-   **Project name:** Distributed Job Scheduler
-   **Repository:** `distributed-job-scheduler`
-   **Category:** Backend infrastructure / distributed systems
-   **Primary goal:** Build a placement-focused Java backend project
    that is clearly different from CRUD-oriented management
    applications.
-   **Target difficulty:** Intermediate
-   **Primary career objective:** Java Backend Developer placement
    preparation.
-   **Core idea:** Developers submit background jobs through an API. The
    platform schedules, dispatches, executes, tracks, and retries those
    jobs using a scheduler, Kafka, and worker processes.

This is not an order, inventory, learning, or content management system.
Its central engineering problem is reliable asynchronous job execution.

## 2. User and development environment

-   Windows 11
-   PowerShell is the preferred shell.
-   Java 21
-   Spring Boot
-   Maven
-   IntelliJ IDEA / VS Code
-   MySQL
-   Apache Kafka
-   Docker and Docker Compose
-   Git and GitHub
-   Typical project parent directory:
    `D:\IdeaProjects\LearningBoot\Projects\`
-   Planned local project directory:
    `D:\IdeaProjects\LearningBoot\Projects\distributed-job-scheduler`

Give exact Windows PowerShell commands and exact file paths. Do not
assume a command, dependency, class, or configuration already exists.

## 3. User's current skill context

The user is a B.Tech CSE student (2023--2027), targeting Java backend
fresher roles. They have worked with Java, Spring Boot, MySQL, MongoDB,
Docker, REST APIs, and basic Kafka, but are still developing depth. They
have already built and verified a small Kafka + Spring Boot + MySQL
pipeline in another project.

Explain important design decisions clearly without assuming
distributed-systems expertise. Avoid dumping the whole application at
once. Work in small phases, verify each phase, and explain the purpose
of each class and component.

## 4. Existing projects and differentiation

The user already has projects in the management/application category,
including: - DevDiary --- content/blog-oriented application with Spring
Boot. - LMS --- learning management application with Spring Boot, JSP,
and MySQL.

The Distributed Job Scheduler was selected specifically to avoid
building another management system. Emphasize infrastructure concerns:
scheduling, asynchronous execution, worker coordination, concurrency,
retries, idempotency, and recovery.

## 5. Product definition

A developer/client submits a job with a job type, payload, scheduled
time, and retry policy. The platform stores it, waits until it is due,
dispatches it through Kafka, and a worker executes the corresponding
supported task handler. The result and execution history are persisted.

Examples: - Generate a daily report at midnight. - Send a reminder after
a delay. - Process a batch of records in the background. - Retry a
transiently failed task after a configured delay.

For the initial version, support a small set of predefined job types
(for example, `REPORT`, `EMAIL`, and `DATA_PROCESSING`). Do **not**
execute arbitrary user-uploaded Java code or scripts. That creates
unnecessary security and sandboxing requirements.

## 6. Agreed technology direction

### Core stack

-   Java 21
-   Spring Boot
-   Spring Web and Jakarta Bean Validation
-   Spring Data JPA / Hibernate
-   MySQL
-   Apache Kafka
-   Spring Kafka
-   Maven
-   Docker Compose
-   Git / GitHub
-   Postman or IntelliJ HTTP Client

### Later, only if justified

-   Redis for distributed coordination, caching, or short-lived state.
-   Spring Security for authentication/authorization.
-   Springdoc OpenAPI / Swagger for API documentation.
-   Spring Boot Actuator and Micrometer for health and metrics.
-   Testcontainers for integration tests.
-   A small frontend/dashboard only after backend correctness.

Do not add every technology at the beginning. Start with the smallest
working architecture and introduce infrastructure when a concrete
requirement needs it.

## 7. Architecture direction

The current design is a **modular backend with independently runnable
worker processes**, not a commitment to many microservices.

Logical responsibilities: 1. **Scheduler API:** Accepts jobs, validates
requests, exposes job status and cancellation/query APIs. 2. **Scheduler
core / dispatcher:** Finds due jobs, claims them safely, and publishes
execution messages. 3. **MySQL:** Stores job definitions, status, retry
metadata, and execution history. 4. **Kafka:** Transports job execution
messages and, later, retry/dead-letter messages. 5. **Workers:** Consume
messages, execute supported job handlers, and persist outcomes. 6.
**Observability:** Logs, correlation IDs, health checks, and metrics as
the project matures.

Initially, keep the API and scheduler core together where practical. Run
worker processes separately when the execution architecture is ready. Do
not create multiple independently deployed services solely to make the
diagram look complex.

## 8. Initial job lifecycle

Proposed states: - `SCHEDULED` --- waiting for its execution time. -
`QUEUED` --- claimed/selected for dispatch and published for
execution. - `RUNNING` --- a worker has claimed the execution. -
`COMPLETED` --- execution succeeded. - `RETRY_PENDING` --- a retry has
been scheduled after a retryable failure. - `FAILED` --- retries are
exhausted or a non-retryable execution failure occurred. - `CANCELLED`
--- cancelled before execution/claim, subject to defined cancellation
rules.

Illustrative transitions: -
`SCHEDULED -> QUEUED -> RUNNING -> COMPLETED` -
`RUNNING -> RETRY_PENDING -> QUEUED -> RUNNING` - `RUNNING -> FAILED` -
`SCHEDULED -> CANCELLED` - `QUEUED -> CANCELLED` only if it has not been
claimed for execution and the implementation can safely enforce this.

The exact state machine and transition ownership must be finalized in
Phase 0 before implementation. API clients must not be allowed to
arbitrarily set internal job states.

## 9. Core engineering requirements

### Scheduling

-   Support one-time scheduled jobs first.
-   Add recurring jobs only after one-time scheduling works.
-   Define timezone behavior explicitly; persist timestamps consistently
    (prefer UTC).
-   Avoid relying on an in-memory timer as the only source of truth.
    Jobs must survive application restarts.

### Safe dispatch and concurrency

-   Multiple scheduler instances must not dispatch the same due job
    concurrently.
-   Use database-backed atomic claiming/state transitions (for example,
    conditional updates or row locking).
-   Define what happens if the dispatcher crashes after claiming a job
    but before publishing it.
-   Consider a transactional outbox when the basic flow works; do not
    claim atomic database-plus-Kafka writes without it.

### Worker execution

-   Workers execute only supported, registered job handlers.
-   Multiple workers should be able to process different jobs
    concurrently.
-   Persist execution attempts and results.
-   Define execution timeout and abandoned-job recovery as later
    reliability work.

### Idempotency

-   Kafka delivery can be repeated.
-   Workers must safely handle duplicate messages using a stable
    job/execution identifier and persisted processing state.
-   Do not claim end-to-end exactly-once execution merely because Kafka
    has exactly-once features.

### Retries and dead-letter handling

-   Retry only transient failures.
-   Use bounded retries and a clear backoff policy.
-   Do not retry permanent validation or unsupported-job errors forever.
-   After retry exhaustion, mark the job failed and make the failure
    inspectable.
-   A dead-letter topic can be added when the core flow works.

### Testing

Test behavior, not just HTTP status codes: - Normal scheduled
execution. - Job not executed before its scheduled time. - Job executed
after becoming due. - Duplicate Kafka message. - Two dispatchers
attempting to claim the same job. - Worker failure and retry. - Retry
exhaustion. - Worker restart / abandoned execution recovery. -
Cancellation before execution. - Multiple workers processing independent
jobs.

## 10. API direction (provisional)

Example submission:

`POST /api/jobs`

``` json
{
  "jobName": "GenerateDailyReport",
  "jobType": "REPORT",
  "payload": {
    "reportType": "SALES"
  },
  "scheduledAt": "2026-11-26T00:00:00",
  "maxRetries": 3
}
```

This is an illustrative contract, not a finalized schema. Use a
validated request DTO, not a JPA entity, as the API contract.

Likely API capabilities: - Submit a job. - Get job details/status. -
List jobs with pagination and filters. - Cancel an eligible job. - View
execution attempts/history. - Optionally retry a failed job manually,
with explicit safeguards.

Finalize endpoint names, payload schema, timezone convention, validation
rules, and response codes in Phase 0.

## 11. Planned repository structure

The previously proposed logical structure:

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
│   ├── requirements.md
│   ├── architecture.md
│   ├── job-lifecycle.md
│   └── api-documentation.md
├── postman/
├── README.md
└── .gitignore
```

This is a proposed layout, not an instruction to create empty modules
prematurely. The actual Maven structure should be selected deliberately
in Phase 0. A single Spring Boot Maven application with clear packages
plus a separately runnable worker profile/process may be simpler
initially.

## 12. Phased implementation roadmap

### Phase 0 --- Requirements and design

-   Confirm MVP scope and supported job types.
-   Identify actors and use cases.
-   Finalize functional and non-functional requirements.
-   Finalize job lifecycle and transition rules.
-   Draw architecture and sequence diagrams.
-   Decide Maven/module structure and local Docker topology.
-   Create GitHub repository, README, and docs.

### Phase 1 --- Foundation

-   Create Spring Boot project using Java 21 and Maven.
-   Configure MySQL.
-   Add Flyway migrations.
-   Create job and execution-attempt models.
-   Add request/response DTOs, validation, repository, service, and
    controller.
-   Implement submit/get/list APIs.
-   Add unit and repository tests.

### Phase 2 --- Scheduling and dispatch

-   Implement persistent due-job detection.
-   Implement safe database-backed job claiming.
-   Ensure a restart does not lose scheduled jobs.
-   Add tests for due-time behavior and competing dispatchers.

### Phase 3 --- Kafka and workers

-   Define versioned execution event DTOs separate from JPA entities.
-   Publish due jobs to Kafka.
-   Implement worker consumers and registered job handlers.
-   Persist execution outcomes.
-   Demonstrate multiple workers.

### Phase 4 --- Reliability

-   Add idempotent processing.
-   Add bounded retries and backoff.
-   Add dead-letter handling.
-   Handle worker crash and abandoned-job recovery.
-   Consider the transactional outbox to address the database/Kafka
    dual-write problem.

### Phase 5 --- Concurrency and operational quality

-   Add concurrent dispatcher and worker tests.
-   Add cancellation safeguards.
-   Add structured logs and correlation IDs.
-   Add health checks and useful metrics.

### Phase 6 --- Portfolio readiness

-   Add Swagger/OpenAPI.
-   Complete integration tests; use Testcontainers if practical.
-   Document architecture, API examples, state machine, failure
    recovery, and trade-offs.
-   Provide Docker Compose startup instructions.
-   Record a short demo showing normal execution and failure/retry
    behavior.
-   Deploy only after local reliability is demonstrated.

## 13. Working rules for ChatGPT

1.  Treat this document as the continuity source for the project.
2.  At the start of a new session, inspect the latest project
    files/repository state if available. Do not assume the user
    completed a previous step.
3.  Work phase by phase; do not dump the entire application at once.
4.  Give exact file paths, package names, complete code for the current
    step, and PowerShell commands.
5.  Explain why each class/configuration exists and how it participates
    in the flow.
6.  State prerequisites and dependencies explicitly.
7.  After each implementation step, provide a concrete test and expected
    result.
8.  Diagnose actual errors from the user's logs/code; do not suggest
    unrelated rewrites.
9.  Keep the README and Git commits updated.
10. Be candid about what is production-grade versus a local
    demonstration.
11. Do not introduce Redis, security, a frontend, or multiple
    microservices without a concrete reason.
12. Do not claim exactly-once business processing without explaining the
    database, Kafka, and idempotency boundaries.
13. When a design is still provisional, label it provisional rather than
    silently treating it as agreed.
14. Preserve the user's learning: explain concepts and ask them to
    run/test steps rather than simply generating a finished repository.

## 14. Success criteria

The project is meaningfully complete when it can demonstrate: - Jobs can
be submitted and persisted. - One-time jobs execute at the intended
time, including after application restart. - Multiple workers can
execute independent jobs. - Concurrent dispatchers do not claim the same
job. - Duplicate Kafka delivery does not repeat the protected business
operation. - Transient failures are retried within a bounded policy. -
Permanent failures and exhausted retries are inspectable. - Worker
restart/recovery behavior is tested. - API, architecture, local setup,
and trade-offs are documented.

A large number of services, endpoints, or technologies is not a success
criterion. Correctness, reliability, test evidence, and the ability to
explain design choices matter more.

## 15. Current project status

At the point this context was created: - Project direction selected:
Distributed Job Scheduler. - Goal: placement-focused. - Difficulty:
intermediate. - Technology direction: Java 21, Spring Boot, MySQL,
Kafka, Maven, Docker Compose. - Repository name selected:
`distributed-job-scheduler`. - Initial GitHub creation and clone were
instructed, but completion has **not yet been verified**. - No
implementation should be assumed to exist. - Immediate next action:
confirm GitHub repository creation/clone/initial commit, then complete
Phase 0 requirements and design before writing application code.

**Update this section after every meaningful milestone.**
