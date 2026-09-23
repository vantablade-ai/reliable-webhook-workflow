from app.core.clock import Clock
from app.core.hashing import payload_fingerprint
from app.models import OrderEvent
from app.repositories.workflow import WorkflowRepository


class EventConflict(Exception):
    pass


class IntakeService:
    def __init__(self, repository: WorkflowRepository, clock: Clock, max_attempts: int = 4) -> None:
        self.repository, self.clock, self.max_attempts = repository, clock, max_attempts

    def accept(self, event: OrderEvent) -> tuple[str, str, bool]:
        event_id, job_id, duplicate, conflict = self.repository.accept(event, payload_fingerprint(event), self.clock.now(), self.max_attempts)
        if conflict:
            raise EventConflict(event_id)
        return event_id, job_id, duplicate
