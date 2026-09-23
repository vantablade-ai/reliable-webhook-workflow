from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.core.hashing import payload_fingerprint
from app.core.retry import FixedJitter, RetryPolicy
from app.downstream.fake import FakeDownstreamAdapter
from app.main import app
from app.models import OrderEvent
from app.security import sign_body, verify_signature
from app.services.intake import EventConflict, IntakeService
from app.services.recovery import RecoveryService
from app.services.worker import WorkerService


def event(**updates):
    data = {"event_id": "evt1", "event_type": "order.created", "order_id": "ord1", "customer_ref": "demo", "amount_cents": 4900, "currency": "usd"}
    data.update(updates)
    return OrderEvent(**data)


def make_job(repo, clock, ev=None, attempts=4):
    return IntakeService(repo, clock, attempts).accept(ev or event())


def test_signatures_use_raw_body_and_reject_bad_values():
    raw = b'{ "a": 1 }'
    signature = sign_body(raw, "secret")
    assert verify_signature(raw, signature, "secret")
    assert not verify_signature(raw, None, "secret")
    assert not verify_signature(raw, "bad", "secret")
    assert not verify_signature(raw + b" ", signature, "secret")
    assert not verify_signature(raw, signature, "wrong")


def test_fingerprint_ignores_object_key_order():
    left = OrderEvent.model_validate({"event_id":"a", "event_type":"order.created", "order_id":"b", "customer_ref":"c", "amount_cents":1, "currency":"USD"})
    right = OrderEvent.model_validate({"currency":"USD", "amount_cents":1, "customer_ref":"c", "order_id":"b", "event_type":"order.created", "event_id":"a"})
    assert payload_fingerprint(left) == payload_fingerprint(right)


def test_first_delivery_duplicate_and_conflict_are_atomic(repo, clock):
    event_id, job_id, duplicate = make_job(repo, clock)
    assert (event_id, duplicate) == ("evt1", False)
    assert make_job(repo, clock) == (event_id, job_id, True)
    with pytest.raises(EventConflict):
        make_job(repo, clock, event(amount_cents=5000))
    assert len(repo.list_jobs()) == 1
    assert repo.get_event("evt1")["payload_hash"] == payload_fingerprint(event())


def test_atomic_claim_sets_lease_and_only_one_wins(repo, clock):
    make_job(repo, clock)
    a = repo.claim("worker-a", clock.now(), 30)
    b = repo.claim("worker-b", clock.now(), 30)
    assert a and a["worker_id"] == "worker-a"
    assert b is None
    assert a["lease_expires_at"] == (clock.now() + timedelta(seconds=30)).isoformat()


def test_future_retry_cannot_be_claimed(repo, clock):
    make_job(repo, clock)
    job = repo.claim("w", clock.now(), 20)
    attempt = repo.start_attempt(job, clock.now())
    from app.core.state import JobStatus
    repo.finish_attempt(attempt, job["job_id"], "w", clock.now(), "RETRYABLE", "timeout", "TimeoutError", None,
                        JobStatus.RETRY_WAIT, clock.now()+timedelta(seconds=4))
    assert repo.claim("w2", clock.now(), 20) is None


def test_worker_success_and_attempt_audit(repo, clock):
    make_job(repo, clock)
    service = WorkerService(repo, FakeDownstreamAdapter(["success"]), clock)
    result = service.process_one("worker")
    assert result["result"] == "succeeded"
    job = repo.get_job(result["job_id"])
    assert job["completed_at"] == clock.now().isoformat()
    assert repo.attempts(job["job_id"])[0]["outcome"] == "SUCCESS"


def test_retry_schedule_and_exhaustion(repo, clock):
    make_job(repo, clock, attempts=3)
    worker = WorkerService(repo, FakeDownstreamAdapter(["rate_limit", "server_error", "timeout"]), clock,
                           RetryPolicy(1, 8, 2), FixedJitter(0.5))
    for delay in (1.5, 2.5):
        result = worker.process_one("w")
        assert result["result"] == "retry_wait"
        assert repo.get_job(result["job_id"])["attempt_count"] in (1, 2)
        clock.advance(delay)
    result = worker.process_one("w")
    assert result["result"] == "dead_letter"
    assert len(repo.attempts(result["job_id"])) == 3


def test_terminal_failure_dead_letters_immediately(repo, clock):
    make_job(repo, clock)
    result = WorkerService(repo, FakeDownstreamAdapter(["permanent_failure"]), clock).process_one("w")
    assert result["result"] == "dead_letter"
    assert repo.get_job(result["job_id"])["attempt_count"] == 1


def test_crash_lease_recovery_and_retry_claim(repo, clock):
    make_job(repo, clock)
    worker = WorkerService(repo, FakeDownstreamAdapter(["crash"]), clock, lease_seconds=5)
    with pytest.raises(SystemExit):
        worker.process_one("worker-a")
    job = repo.list_jobs()[0]
    assert job["status"] == "RUNNING"
    assert RecoveryService(repo, clock).recover() == []
    clock.advance(6)
    assert RecoveryService(repo, clock).recover() == [job["job_id"]]
    assert len(repo.attempts(job["job_id"])) == 1
    result = WorkerService(repo, FakeDownstreamAdapter(["success"]), clock).process_one("worker-b")
    assert result["result"] == "succeeded"
    assert len(repo.attempts(job["job_id"])) == 2


def test_api_health_and_missing_jobs():
    client = TestClient(app)
    assert client.get("/health").json() == {"status":"ok"}
    assert client.get("/jobs/not-a-job").status_code == 404


def test_model_rejects_extra_and_invalid_event():
    with pytest.raises(ValidationError):
        event(unexpected="field")
    with pytest.raises(ValidationError):
        event(amount_cents=-1)
