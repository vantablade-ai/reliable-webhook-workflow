import json
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from app.core.state import JobStatus, require_transition
from app.models import OrderEvent

SCHEMA = Path(__file__).resolve().parents[2] / "schema.sql"


def iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class WorkflowRepository:
    def __init__(self, path: str = "workflow.db") -> None:
        self.path = path
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def _initialize(self) -> None:
        with self.connect() as db:
            db.executescript(SCHEMA.read_text())

    def accept(self, event: OrderEvent, payload_hash: str, now: datetime, max_attempts: int) -> tuple[str, str, bool, bool]:
        """Returns event id, job id, duplicate, conflict."""
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute("SELECT event_id, payload_hash, job_id FROM webhook_events WHERE event_id=?", (event.event_id,)).fetchone()
            if existing:
                db.commit()
                return event.event_id, existing["job_id"], True, existing["payload_hash"] != payload_hash
            job_id = str(uuid.uuid4())
            db.execute("INSERT INTO webhook_events(event_id,event_type,payload_json,payload_hash,received_at,job_id) VALUES(?,?,?,?,?,?)",
                       (event.event_id, event.event_type, json.dumps(event.model_dump(mode="json"), sort_keys=True), payload_hash, iso(now), job_id))
            db.execute("INSERT INTO jobs(job_id,event_id,status,attempt_count,max_attempts,next_attempt_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                       (job_id, event.event_id, JobStatus.QUEUED, 0, max_attempts, iso(now), iso(now), iso(now)))
            db.commit()
            return event.event_id, job_id, False, False
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def claim(self, worker_id: str, now: datetime, lease_seconds: int) -> dict[str, Any] | None:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            candidate = db.execute("SELECT * FROM jobs WHERE status IN (?,?) AND next_attempt_at<=? ORDER BY next_attempt_at,created_at LIMIT 1",
                                   (JobStatus.QUEUED, JobStatus.RETRY_WAIT, iso(now))).fetchone()
            if not candidate:
                db.commit()
                return None
            until = now + timedelta(seconds=lease_seconds)
            updated = db.execute("UPDATE jobs SET status=?,worker_id=?,claimed_at=?,lease_expires_at=?,updated_at=? WHERE job_id=? AND status IN (?,?) AND next_attempt_at<=?",
                                 (JobStatus.RUNNING, worker_id, iso(now), iso(until), iso(now), candidate["job_id"], JobStatus.QUEUED, JobStatus.RETRY_WAIT, iso(now))).rowcount
            if updated != 1:
                db.rollback()
                return None
            db.commit()
            result = dict(candidate)
            result.update(status=JobStatus.RUNNING, worker_id=worker_id, claimed_at=iso(now), lease_expires_at=iso(until))
            event = db.execute("SELECT payload_json FROM webhook_events WHERE event_id=?", (result["event_id"],)).fetchone()
            result["payload"] = json.loads(event["payload_json"])
            return result
        finally:
            db.close()

    def start_attempt(self, job: dict[str, Any], now: datetime) -> int:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            number = int(job["attempt_count"]) + 1
            cur = db.execute("UPDATE jobs SET attempt_count=?,updated_at=? WHERE job_id=? AND status=? AND worker_id=?",
                             (number, iso(now), job["job_id"], JobStatus.RUNNING, job["worker_id"]))
            if cur.rowcount != 1:
                raise RuntimeError("claim ownership lost before attempt start")
            cur = db.execute("INSERT INTO job_attempts(job_id,attempt_number,worker_id,started_at) VALUES(?,?,?,?)",
                             (job["job_id"], number, job["worker_id"], iso(now)))
            db.commit()
            return int(cur.lastrowid)
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def finish_attempt(self, attempt_id: int, job_id: str, worker_id: str, now: datetime, outcome: str,
                       error_code: str | None, error_class: str | None, status_code: int | None,
                       target_status: JobStatus, retry_at: datetime | None = None) -> None:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT status,worker_id FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if not row or row["status"] != JobStatus.RUNNING or row["worker_id"] != worker_id:
                raise RuntimeError("worker no longer owns job")
            require_transition(JobStatus.RUNNING, target_status)
            db.execute("UPDATE job_attempts SET finished_at=?,outcome=?,error_code=?,error_class=?,downstream_status=?,scheduled_retry_at=? WHERE attempt_id=?",
                       (iso(now), outcome, error_code, error_class, status_code, iso(retry_at), attempt_id))
            completed = iso(now) if target_status in (JobStatus.SUCCEEDED, JobStatus.DEAD_LETTER) else None
            db.execute("UPDATE jobs SET status=?,next_attempt_at=?,claimed_at=NULL,lease_expires_at=NULL,worker_id=NULL,last_error=?,final_reason=?,completed_at=?,updated_at=? WHERE job_id=?",
                       (target_status, iso(retry_at) if retry_at else iso(now), error_code, error_code if target_status == JobStatus.DEAD_LETTER else None, completed, iso(now), job_id))
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def recover_expired(self, now: datetime) -> list[str]:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            rows = db.execute("SELECT job_id FROM jobs WHERE status=? AND lease_expires_at<? ORDER BY lease_expires_at", (JobStatus.RUNNING, iso(now))).fetchall()
            ids = [r["job_id"] for r in rows]
            for job_id in ids:
                db.execute("UPDATE jobs SET status=?,worker_id=NULL,claimed_at=NULL,lease_expires_at=NULL,next_attempt_at=?,updated_at=?,recovery_count=recovery_count+1,last_error='lease_expired' WHERE job_id=? AND status=? AND lease_expires_at<?",
                           (JobStatus.QUEUED, iso(now), iso(now), job_id, JobStatus.RUNNING, iso(now)))
                db.execute("UPDATE job_attempts SET finished_at=?,outcome='CRASH',error_code='lease_expired',error_class='WorkerLeaseExpired' WHERE job_id=? AND finished_at IS NULL", (iso(now), job_id))
            db.commit()
            return ids
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM webhook_events WHERE event_id=?", (event_id,)).fetchone()
            return dict(row) if row else None

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            return dict(row) if row else None

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,))]

    def attempts(self, job_id: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM job_attempts WHERE job_id=? ORDER BY attempt_number", (job_id,))]
