import random
from typing import Protocol


class JitterSource(Protocol):
    def seconds(self, maximum: float) -> float: ...


class RandomJitter:
    def seconds(self, maximum: float) -> float:
        return random.uniform(0, maximum)


class FixedJitter:
    def __init__(self, value: float = 0) -> None:
        self.value = value

    def seconds(self, maximum: float) -> float:
        return min(self.value, maximum)


class RetryPolicy:
    def __init__(self, base_seconds: float = 1, max_seconds: float = 60, jitter_seconds: float = 0) -> None:
        self.base_seconds, self.max_seconds, self.jitter_seconds = base_seconds, max_seconds, jitter_seconds

    def delay(self, attempt: int, jitter: JitterSource) -> float:
        return min(self.base_seconds * (2 ** (attempt - 1)), self.max_seconds) + jitter.seconds(self.jitter_seconds)
