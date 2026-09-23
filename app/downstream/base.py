from typing import Protocol

from app.models import DownstreamResult


class DownstreamAdapter(Protocol):
    def deliver(self, payload: dict[str, object]) -> DownstreamResult: ...
