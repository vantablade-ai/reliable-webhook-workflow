PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS webhook_events (
  event_id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at TEXT NOT NULL,
  job_id TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE REFERENCES webhook_events(event_id),
  status TEXT NOT NULL CHECK(status IN ('QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','DEAD_LETTER')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL,
  next_attempt_at TEXT NOT NULL,
  claimed_at TEXT,
  lease_expires_at TEXT,
  worker_id TEXT,
  last_error TEXT,
  final_reason TEXT,
  recovery_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);
CREATE TABLE IF NOT EXISTS job_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL REFERENCES jobs(job_id),
  attempt_number INTEGER NOT NULL,
  worker_id TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  outcome TEXT,
  error_code TEXT,
  error_class TEXT,
  downstream_status INTEGER,
  scheduled_retry_at TEXT,
  UNIQUE(job_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS idx_jobs_due ON jobs(status,next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(status,lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_jobs_event ON jobs(event_id);
CREATE INDEX IF NOT EXISTS idx_attempts_job ON job_attempts(job_id,attempt_number);
