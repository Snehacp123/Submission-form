# Submission API with Live Aggregation

A Go REST API for form submissions (PostgreSQL), a load test, and an Apache Flink
job that keeps a live count of submissions per form, per hour.

```
                         ┌──────────────────────────── Postgres ────────────────────────────┐
 client ──HTTP──▶ Go API ─┼─▶ submissions ──WAL (logical decoding)──┐                       │
                         │                                          ▼                       │
                         │   form_hourly_counts ◀──upsert── Flink job (CDC → GROUP BY)      │
                         └──────────────────────────────────────────────────────────────────┘
                                   ▲
 browser / API ──GET /forms/{id}/hourly-counts
```

## Quick start (everything in Docker)

Requires Docker Desktop (about 3 GB RAM free) and Go 1.25+ for the load test.

```powershell
docker compose up -d --build              # Postgres, API, Flink JobManager + TaskManager
docker compose run --rm flink-job         # submit the Flink SQL job (once)
```

| What | Where |
|---|---|
| API and web page | http://localhost:8080 |
| Flink dashboard | http://localhost:8082 |
| Postgres | `localhost:5435`, user/password `postgres`/`postgres`, db `forms` |

If a port is taken, override it, e.g. `$env:API_PORT="8090"; docker compose up -d`.

Stop everything with `docker compose down` (add `-v` to delete the data too).

Running the API without Docker also works (`go run ./cmd/api`, configured from
`.env`; see `.env.example`), but Part 3 needs the Docker Postgres, which has
`wal_level=logical` turned on.

## Part 1: the API

| Endpoint | Behaviour |
|---|---|
| `POST /submissions` | Validates the body, stores it, returns **201** with the record. Repeating an `idempotency_key` returns **200** with the *original* record and header `Idempotent-Replayed: true`. Invalid input returns **400** with per-field errors. |
| `GET /forms/{form_id}/submissions?page=N` | 10 per page, ordered by id, with `page`, `per_page`, `total`, `total_pages`. |
| `GET /forms/{form_id}/hourly-counts` | Part 3 output: Flink's hourly counts for the form. |
| `GET /healthz` | `ok` if the database is reachable. |
| `GET /` | A small page to submit forms, browse submissions and watch the live counts. |

### Design choices

- **Duplicate protection is enforced by the database.** `idempotency_key` has a
  `UNIQUE` constraint, and the insert is `INSERT … ON CONFLICT DO NOTHING RETURNING`.
  If nothing comes back, the key already exists and a second statement fetches the
  existing row. A check-then-insert in Go would let two concurrent requests with the
  same key both pass the check; the constraint can't be raced. The fetch is a
  separate statement on purpose: under READ COMMITTED it takes a fresh snapshot,
  so it always sees a row committed by a concurrent request
  (`internal/submission/repository.go`).
- **Validation** (`internal/submission/validate.go`): `form_id` and
  `idempotency_key` are required (trimmed, max 255 chars). `email` must be a bare
  address with a dotted domain; `mail.ParseAddress` alone would accept
  `John <john@example.com>`. `data` must be a JSON object (missing means `{}`). All
  errors are returned together, and the body is capped at 1 MiB.
- **Pagination** uses `LIMIT/OFFSET` backed by an index on `(form_id, id)`. At this
  scale that's simplest. For very deep pages, keyset pagination (`WHERE id > last_id`)
  would be the next step.
- **Operational details:** server timeouts, graceful shutdown on Ctrl+C/SIGTERM, a
  configurable connection pool, and JSON logs.

### Check it

```powershell
powershell -File scripts\smoke-test.ps1        # valid, duplicate, invalid, pagination
go test ./...                                  # handler tests with an in-memory store
```

Or import `postman/form-submission.postman_collection.json` and use **Run collection**.
Every request has assertions.

## Part 2: load test

`cmd/loadtest` sends 100,000 `POST /submissions`: 80,000 unique keys plus 20,000
requests that reuse those keys with the same payload, as a retrying client would.
They're shuffled so duplicates race their originals. It counts responses by status,
then queries the database directly to confirm what was actually stored.

```powershell
go run ./cmd/loadtest -total 100000 -dups 20000 -c 200 -dsn "postgres://postgres:postgres@localhost:5435/forms?sslmode=disable"
```

### Results

Run on 2026-11-06 against the Docker stack, on a Windows 11 laptop (12 cores, Docker
limited to 8 GB), with 200 concurrent workers:

| Metric | Result |
|---|---|
| Requests sent | 100,000 |
| Submissions stored (201) | **80,000** (expected 80,000) |
| Duplicates correctly rejected (200, original returned) | **20,000** (expected 20,000) |
| Errors | **0** (4xx 0, 5xx 0, network 0) |
| Throughput | **≈ 4,980 requests/second** (20.1 s total) |
| Latency p50 / p95 / p99 | 33.7 ms / 82.4 ms / 130.8 ms |
| Database check | 80,000 rows for the run, 80,000 distinct keys, so no duplicates |

Throughput depends on the machine. The correctness numbers shouldn't.

## Part 3: live aggregation with Flink

### How it works

The job is Flink SQL (`flink/sql/hourly_counts.sql`):

1. **Source: Postgres change data capture** (`postgres-cdc` connector). Flink reads
   the `submissions` table's changes from the Postgres write-ahead log through a
   replication slot. On first start it reads the existing rows, then follows new
   changes continuously.
2. **Aggregation:** `GROUP BY form_id, FLOOR(created_at TO HOUR)` with `COUNT(*)`.
   This is a continuously updating aggregate, not a tumbling window, so the current
   hour's count changes with every submission instead of appearing only when the hour
   closes. Late or backdated rows land in the correct hour too.
3. **Sink:** JDBC upsert into `form_hourly_counts (form_id, hour_start, submission_count)`
   with primary key `(form_id, hour_start)`.

**Why CDC instead of having the API publish events (to Kafka, for example):**
publishing from the API is a *dual write*. A crash between the database commit and
the publish loses an event, and publishing first can emit an event for a row that was
never committed. The WAL contains exactly the committed rows, so Flink sees every
stored submission once. The API doesn't need to change, and duplicates rejected by the
API never reach Flink, because they never create a row.

### Crash safety: no lost counts, no double counts

- Every 10 s Flink takes a **checkpoint** (`EXACTLY_ONCE` mode) that saves, together,
  **the running counts** and **the WAL position (LSN) the source has read up to**.
  Because both are saved at the same moment, they always agree.
- The CDC source only confirms an LSN to Postgres **after a checkpoint completes**.
  Until then, Postgres keeps that part of the WAL in the replication slot, so nothing
  Flink hasn't saved can be discarded.
- **After a crash**, Flink restores the latest checkpoint, getting back both the
  counts and the LSN, then re-reads the WAL from that LSN.
  - **Nothing is lost:** every change after the checkpoint is still in the WAL and is
    read again.
  - **Nothing is double-counted:** the restored counts reflect exactly the changes up
    to that LSN, so each replayed change is counted once.
- **The sink is idempotent.** It writes the *full* count for a `(form, hour)`
  (`INSERT … ON CONFLICT DO UPDATE SET submission_count = <total>`), never
  `count = count + n`. Rows written between the checkpoint and the crash get written
  again during the replay with the correct value. At-least-once delivery of absolute
  values gives exactly-once results, without needing a transactional sink.
- Deletes and updates are handled. The table uses `REPLICA IDENTITY FULL`, so a
  deleted row arrives with its `form_id` and `created_at`, and Flink retracts it from
  the right group.

**What's covered here:** a TaskManager crash (the process doing the work), which the
test below demonstrates. Flink's restart strategy retries every 5 s until a
TaskManager is available. **A JobManager crash** isn't automatically recovered in
this single-node setup. In production you'd run JobManager high availability
(Kubernetes or ZooKeeper), which stores the job and the latest checkpoint pointer
durably. Checkpoints here are retained on cancellation, so you can resume by hand by
adding `SET 'execution.savepoint.path' = '/opt/flink/checkpoints/<job-id>/chk-<n>';`
before the `INSERT` and submitting again. Even a full restart *without* a checkpoint
gives correct numbers: the job re-reads the table, recomputes, and overwrites every
row with the full count.

### Check it

```powershell
powershell -File scripts\part3-demo.ps1         # live counts growing across several hours
powershell -File scripts\part3-crash-test.ps1   # kill the TaskManager under load, verify counts
```

- **Demo:** seeds a few rows 1 and 2 hours in the past (the API always stamps `now()`),
  then posts submissions in rounds and prints `form_hourly_counts` after each round.
  Each re-sent duplicate leaves the counts unchanged. You can also open
  http://localhost:8080, put the form ID in the Submissions box, and watch the
  **Live hourly counts** section (refreshes every 2 s).
- **Crash test:** sends 20,000 requests (4,000 duplicates) and runs `docker kill` on the
  TaskManager mid-stream. It then starts a new TaskManager, waits for Flink to restore
  from its checkpoint, and compares Flink's counts with a direct
  `count(*) … GROUP BY form_id, date_trunc('hour', created_at)` over `submissions`
  (`scripts/verify-counts.sql`).

Crash test result (2026-11-06):

```
Recoveries during test:     1 (restored from checkpoint #21)
Rows in submissions:        32015
Total counted by Flink:     32015
Mismatched (form, hour):    0
Result:                     PASS - nothing lost, nothing double-counted
```

After the 100k load test, Flink had also counted every one of the 112,015 rows, with
0 mismatched groups.

### Notes and trade-offs

- **Hours are UTC** (`table.local-time-zone = UTC`). In India (UTC+5:30) the buckets
  show as `hh:30` local time. A per-form time zone would be an extra column in the key.
- **Replication slot retention:** if the Flink job stays down, Postgres keeps WAL for
  the slot `flink_hourly_counts`, which uses disk. In production, monitor it, or drop
  the slot when the job is retired:
  `SELECT pg_drop_replication_slot('flink_hourly_counts');`
- **Parallelism is 1**, which is plenty for this load. The aggregation is keyed by
  `(form_id, hour)`, so it scales out by raising parallelism.
- **Mini-batching (1 s)** groups updates during bursts, so the sink writes one row
  update per group rather than one per submission.

## Project layout

```
cmd/api/                  HTTP server (wiring, config, graceful shutdown)
cmd/loadtest/             Part 2 load generator and DB verification
internal/submission/      Part 1: handlers, validation, Postgres store, tests
internal/stats/           GET /forms/{id}/hourly-counts (reads Flink's output)
internal/web/             The browser page at /
internal/db/schema.sql    Tables (applied on API start and by the Docker init)
deploy/postgres/          CDC setup: replica identity and publication
flink/Dockerfile          Flink 1.19 with the Postgres CDC and JDBC connectors
flink/sql/                The Flink SQL job
scripts/                  Smoke test, Part 3 demo, crash test, count verification
docker-compose.yml        Postgres, API, Flink JobManager/TaskManager, job submitter
```
