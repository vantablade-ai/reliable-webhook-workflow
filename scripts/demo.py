import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.clock import FakeClock
from app.core.retry import FixedJitter, RetryPolicy
from app.downstream.fake import FakeDownstreamAdapter
from app.models import OrderEvent
from app.repositories.workflow import WorkflowRepository
from app.security import sign_body, verify_signature
from app.services.intake import EventConflict, IntakeService
from app.services.recovery import RecoveryService
from app.services.worker import WorkerService

SECRET = "synthetic-demo-secret"


def must(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def accept(repo, clock, payload):
    raw = json.dumps(payload, separators=(",", ":")).encode()
    signature = sign_body(raw, SECRET)
    must(verify_signature(raw, signature, SECRET), "raw-body HMAC verified")
    return IntakeService(repo, clock, 3).accept(OrderEvent.model_validate(json.loads(raw)))


def payload(event_id):
    return {"event_id":event_id,"event_type":"order.created","order_id":f"order-{event_id}","customer_ref":"customer_demo","amount_cents":4900,"currency":"USD"}


def main():
    with tempfile.TemporaryDirectory() as directory:
        repo = WorkflowRepository(str(Path(directory) / "demo.db"))
        clock = FakeClock()
        worker = WorkerService(repo, FakeDownstreamAdapter(["rate_limit", "server_error", "success"]), clock,
                               RetryPolicy(1, 8, 0), FixedJitter(0), 5)
        print("Reliable Webhook Workflow — demo\n")
        event_id, job_id, duplicate = accept(repo, clock, payload("evt-demo-1"))
        must(not duplicate and repo.get_job(job_id), "new event persisted")
        print("[ACCEPTED] signed webhook persisted")
        must(accept(repo, clock, payload("evt-demo-1")) == (event_id, job_id, True), "duplicate reused job")
        print("[DUPLICATE] replay reused existing job")
        try:
            accept(repo, clock, {**payload("evt-demo-1"), "amount_cents": 5000})
            raise AssertionError("changed event should conflict")
        except EventConflict:
            print("[409] changed payload rejected for existing event_id")
        result = worker.process_one("worker-a")
        must(result["result"] == "retry_wait", "429 scheduled")
        print("\n[CLAIMED] worker-a")
        print("[RETRY] 429 -> retry in 1s")
        clock.advance(1)
        result = worker.process_one("worker-a")
        must(result["result"] == "retry_wait", "500 scheduled")
        print("[RETRY] 500 -> retry in 2s")
        clock.advance(2)
        result = worker.process_one("worker-a")
        must(result["result"] == "succeeded", "third delivery succeeded")
        print("[SUCCESS] completed on attempt 3")
        print(f"[AUDIT] {len(repo.attempts(job_id))} attempts persisted")

        _, crash_id, _ = accept(repo, clock, payload("evt-demo-crash"))
        crashing = WorkerService(repo, FakeDownstreamAdapter(["crash"]), clock, lease_seconds=5)
        try:
            crashing.process_one("worker-a")
        except SystemExit:
            print("\n[STALE] worker-a lease expired")
        clock.advance(6)
        recovered = RecoveryService(repo, clock).recover()
        must(crash_id in recovered, "stale claim recovered")
        print("[RECOVERED] job returned to queue")
        result = WorkerService(repo, FakeDownstreamAdapter(["success"]), clock).process_one("worker-b")
        must(result["result"] == "succeeded", "recovered job completed")
        print("[CLAIMED] worker-b")
        print("[SUCCESS] recovered job completed")

        _, dead_id, _ = accept(repo, clock, payload("evt-demo-dead"))
        terminal = WorkerService(repo, FakeDownstreamAdapter(["rate_limit", "rate_limit", "rate_limit"]), clock,
                                 RetryPolicy(1, 8, 0), FixedJitter(0))
        for delay in (0, 1, 2):
            if delay:
                clock.advance(delay)
            result = terminal.process_one("worker-dead")
        must(result["result"] == "dead_letter" and repo.get_job(dead_id)["status"] == "DEAD_LETTER", "retry exhaustion dead-lettered")
        print("\n[DEAD_LETTER] retry budget exhausted")
        print("\nAll workflow invariants passed.")


if __name__ == "__main__":
    main()
