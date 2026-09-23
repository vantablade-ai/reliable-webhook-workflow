from datetime import timedelta

from app.core.clock import Clock
from app.core.retry import JitterSource, RetryPolicy
from app.core.state import JobStatus
from app.downstream.base import DownstreamAdapter
from app.models import Outcome
from app.repositories.workflow import WorkflowRepository


class WorkerService:
    def __init__(self, repository: WorkflowRepository, adapter: DownstreamAdapter, clock: Clock,
                 retry_policy: RetryPolicy | None = None, jitter: JitterSource | None = None, lease_seconds: int = 30) -> None:
        from app.core.retry import FixedJitter
        self.repository, self.adapter, self.clock = repository, adapter, clock
        self.retry_policy, self.jitter, self.lease_seconds = retry_policy or RetryPolicy(), jitter or FixedJitter(), lease_seconds

    def process_one(self, worker_id: str) -> dict[str, object]:
        now = self.clock.now()
        job = self.repository.claim(worker_id, now, self.lease_seconds)
        if not job:
            return {"result": "no_work"}
        attempt_id = self.repository.start_attempt(job, now)
        attempt_number = int(job["attempt_count"]) + 1
        # An interruption leaves the lease and attempt open for stale recovery.
        result = self.adapter.deliver(job["payload"])
        finished = self.clock.now()
        if result.outcome == Outcome.SUCCESS:
            target = JobStatus.SUCCEEDED
            retry_at = None
        elif result.outcome == Outcome.TERMINAL or attempt_number >= int(job["max_attempts"]):
            target = JobStatus.DEAD_LETTER
            retry_at = None
        else:
            target = JobStatus.RETRY_WAIT
            retry_at = finished + timedelta(seconds=self.retry_policy.delay(attempt_number, self.jitter))
        self.repository.finish_attempt(attempt_id, str(job["job_id"]), worker_id, finished, result.outcome,
                                       result.error_code, result.error_class, result.status_code, target, retry_at)
        return {"result": target.value.lower(), "job_id": job["job_id"], "attempt": attempt_number,
                "retry_at": retry_at.isoformat() if retry_at else None}
