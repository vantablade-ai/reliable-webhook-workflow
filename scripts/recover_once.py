import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.core.clock import SystemClock
from app.repositories.workflow import WorkflowRepository
from app.services.recovery import RecoveryService

cfg = settings()
recovered = RecoveryService(WorkflowRepository(cfg.database_path), SystemClock()).recover()
print({"recovered": len(recovered), "job_ids": recovered})
