import asyncio
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import ValidationError

from app.config import Settings
from app.core.hashing import payload_fingerprint
from app.core.retry import FixedJitter, RetryPolicy
from app.core.state import JobStatus
from app.downstream.fake import FakeDownstreamAdapter
from app.main import create_app
from app.models import OrderEvent
from app.repositories.workflow import WorkflowRepository
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


def test_two_threads_contending_for_one_due_job_have_one_winner(repo, clock):
    make_job(repo, clock)
    barrier = threading.Barrier(2)
    worker_repositories = (WorkflowRepository(repo.path), WorkflowRepository(repo.path))

    def claim(worker_id, worker_repository):
        barrier.wait()
        return worker_repository.claim(worker_id, clock.now(), 30)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, ("worker-a", "worker-b"), worker_repositories))

    winners = [claim_result for claim_result in claims if claim_result is not None]
    assert len(winners) == 1
    assert winners[0]["worker_id"] in {"worker-a", "worker-b"}


def test_future_retry_cannot_be_claimed(repo, clock):
    make_job(repo, clock)
    job = repo.claim("w", clock.now(), 20)
    attempt = repo.start_attempt(job, clock.now())
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


def test_api_health_and_missing_jobs(tmp_path):
    api = create_app(WorkflowRepository(str(tmp_path / "api.db")))

    async def check_api():
        async with AsyncClient(transport=ASGITransport(app=api), base_url="http://test") as client:
            assert (await client.get("/health")).json() == {"status": "ok"}
            assert (await client.get("/jobs/not-a-job")).status_code == 404

    asyncio.run(check_api())


def test_webhook_api_boundary_and_read_endpoints(tmp_path):
    secret = "api-test-secret"
    repository = WorkflowRepository(str(tmp_path / "api-boundary.db"))
    api = create_app(repository, Settings(database_path=str(tmp_path / "api-boundary.db"), webhook_secret=secret))
    payload = {
        "event_id": "api-event-1",
        "event_type": "order.created",
        "order_id": "api-order-1",
        "customer_ref": "customer-demo",
        "amount_cents": 2500,
        "currency": "USD",
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    signature = sign_body(raw, secret)

    async def exercise_api():
        async with AsyncClient(transport=ASGITransport(app=api), base_url="http://test") as client:
            accepted = await client.post("/webhooks/orders", content=raw, headers={"X-Webhook-Signature": signature})
            assert accepted.status_code == 202
            job_id = accepted.json()["job_id"]

            replay = await client.post("/webhooks/orders", content=raw, headers={"X-Webhook-Signature": signature})
            assert replay.status_code == 200
            assert replay.json()["job_id"] == job_id

            changed_payload = {**payload, "amount_cents": 3500}
            changed_raw = json.dumps(changed_payload, separators=(",", ":")).encode()
            conflict = await client.post(
                "/webhooks/orders", content=changed_raw, headers={"X-Webhook-Signature": sign_body(changed_raw, secret)}
            )
            assert conflict.status_code == 409

            claimed_job = repository.claim("api-worker", datetime.now(UTC), 30)
            assert claimed_job is not None
            repository.start_attempt(claimed_job, datetime.now(UTC))

            assert (await client.post("/webhooks/orders", content=raw)).status_code == 401
            assert (await client.post(
                "/webhooks/orders", content=raw, headers={"X-Webhook-Signature": "0" * 64}
            )).status_code == 401
            assert (await client.post(
                "/webhooks/orders", content=raw + b" ", headers={"X-Webhook-Signature": signature}
            )).status_code == 401

            malformed = b'{"event_id":'
            assert (await client.post(
                "/webhooks/orders", content=malformed, headers={"X-Webhook-Signature": sign_body(malformed, secret)}
            )).status_code == 422
            invalid_event = {**payload, "amount_cents": -1}
            invalid_raw = json.dumps(invalid_event, separators=(",", ":")).encode()
            assert (await client.post(
                "/webhooks/orders", content=invalid_raw,
                headers={"X-Webhook-Signature": sign_body(invalid_raw, secret)}
            )).status_code == 422

            event_response = await client.get("/events/api-event-1")
            assert event_response.status_code == 200
            assert event_response.json()["payload"]["event_id"] == "api-event-1"
            assert (await client.get("/jobs")).json()[0]["job_id"] == job_id
            assert (await client.get(f"/jobs/{job_id}")).json()["event_id"] == "api-event-1"
            attempts = (await client.get(f"/jobs/{job_id}/attempts")).json()
            assert len(attempts) == 1
            assert attempts[0]["attempt_number"] == 1
            assert (await client.get("/events/missing")).status_code == 404
            assert (await client.get("/jobs/missing")).status_code == 404
            assert (await client.get("/jobs/missing/attempts")).status_code == 404

    asyncio.run(exercise_api())


def test_model_rejects_extra_and_invalid_event():
    with pytest.raises(ValidationError):
        event(unexpected="field")
    with pytest.raises(ValidationError):
        event(amount_cents=-1)
