import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_path: str = "workflow.db"
    webhook_secret: str = "local-demo-secret-change-me"
    lease_seconds: int = 30
    max_attempts: int = 4
    base_backoff_seconds: float = 1
    max_backoff_seconds: float = 60
    jitter_seconds: float = 0


def settings() -> Settings:
    return Settings(
        database_path=os.getenv("DATABASE_PATH", "workflow.db"),
        webhook_secret=os.getenv("WEBHOOK_SECRET", "local-demo-secret-change-me"),
        lease_seconds=int(os.getenv("WORKER_LEASE_SECONDS", "30")),
        max_attempts=int(os.getenv("MAX_ATTEMPTS", "4")),
        base_backoff_seconds=float(os.getenv("BASE_BACKOFF_SECONDS", "1")),
        max_backoff_seconds=float(os.getenv("MAX_BACKOFF_SECONDS", "60")),
        jitter_seconds=float(os.getenv("JITTER_SECONDS", "0")),
    )
