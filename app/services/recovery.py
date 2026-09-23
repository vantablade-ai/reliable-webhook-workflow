from app.core.clock import Clock
from app.repositories.workflow import WorkflowRepository


class RecoveryService:
    def __init__(self, repository: WorkflowRepository, clock: Clock) -> None:
        self.repository, self.clock = repository, clock

    def recover(self) -> list[str]:
        return self.repository.recover_expired(self.clock.now())
