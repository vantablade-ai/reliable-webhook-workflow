# Architecture notes

The receiver trusts a delivery only after verifying `X-Webhook-Signature` against the exact request bytes with HMAC-SHA256. The payload is parsed afterward into a strict Pydantic model. There is no timestamp/replay window because provider protocol freshness is outside this demonstration.

The stored SHA-256 fingerprint is computed from normalized model JSON with sorted object keys. It helps detect event ID reuse with changed semantics; it is not an authentication mechanism. Event insertion and one-job creation commit together under SQLite `BEGIN IMMEDIATE`, with unique constraints providing a second defense.

Jobs move through `QUEUED → RUNNING → SUCCEEDED | RETRY_WAIT | DEAD_LETTER`. Due retry jobs re-enter `RUNNING`. An active claim stores worker ID and lease expiry. Selection and guarded update happen in one immediate transaction, so SQLite serializes contenders and only one transaction can own the row.

The attempt row is opened before calling downstream. A normal result closes it with outcome, HTTP status, error class/code, and retry time. A hard worker death leaves the attempt open; expiry recovery closes it as `CRASH` and returns the job to `QUEUED`. Recovery intentionally permits at-least-once execution, including the possibility that the external side effect happened before the crash.

HTTP 429, 5xx and timeout outcomes are retryable. Explicit permanent rejection is terminal. Retryable failures stop at the configured attempt budget. Delay is `min(base × 2^(attempt−1), cap) + bounded_jitter`; tests inject fixed jitter and a manually advanced clock.

The receiver has no downstream dependency, and its response follows durable persistence. Worker concurrency is bounded by the number of worker processes/operators started; the sample CLI processes one job. SQLite is chosen for local portability and transaction semantics, not horizontal throughput. This proof makes no exactly-once claim: downstream side effects need their own idempotency key in production.
