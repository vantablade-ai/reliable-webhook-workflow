import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.core.clock import SystemClock
from app.core.retry import RandomJitter, RetryPolicy
from app.downstream.fake import FakeDownstreamAdapter
from app.repositories.workflow import WorkflowRepository
from app.services.worker import WorkerService

cfg = settings()
repo = WorkflowRepository(cfg.database_path)
worker = WorkerService(repo, FakeDownstreamAdapter(), SystemClock(),
                       RetryPolicy(cfg.base_backoff_seconds, cfg.max_backoff_seconds, cfg.jitter_seconds),
                       RandomJitter(), cfg.lease_seconds)
print(worker.process_one("worker-cli"))
