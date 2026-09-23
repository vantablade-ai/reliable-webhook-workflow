from enum import StrEnum


class JobStatus(StrEnum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    DEAD_LETTER = "DEAD_LETTER"


TRANSITIONS = {
    JobStatus.QUEUED: {JobStatus.RUNNING},
    JobStatus.RETRY_WAIT: {JobStatus.RUNNING},
    JobStatus.RUNNING: {JobStatus.QUEUED, JobStatus.RETRY_WAIT, JobStatus.SUCCEEDED, JobStatus.DEAD_LETTER},
}


def require_transition(old: JobStatus, new: JobStatus) -> None:
    if new not in TRANSITIONS.get(old, set()):
        raise ValueError(f"invalid job transition: {old} -> {new}")
