from collections import deque

from app.models import DownstreamResult, Outcome


class FakeDownstreamAdapter:
    """Offline scriptable service. CRASH simulates process loss before completion."""
    def __init__(self, scenarios: list[str] | None = None) -> None:
        self.scenarios = deque(scenarios or ["success"])
        self.calls = 0

    def deliver(self, payload: dict[str, object]) -> DownstreamResult:
        self.calls += 1
        scenario = self.scenarios.popleft() if self.scenarios else "success"
        if scenario == "crash":
            raise SystemExit("simulated worker crash")
        if scenario == "success":
            return DownstreamResult(outcome=Outcome.SUCCESS, status_code=200)
        if scenario == "rate_limit":
            return DownstreamResult(outcome=Outcome.RETRYABLE, status_code=429, error_code="http_429", error_class="RateLimited")
        if scenario == "server_error":
            return DownstreamResult(outcome=Outcome.RETRYABLE, status_code=500, error_code="http_500", error_class="ServerError")
        if scenario == "timeout":
            return DownstreamResult(outcome=Outcome.RETRYABLE, error_code="timeout", error_class="TimeoutError")
        if scenario == "permanent_failure":
            return DownstreamResult(outcome=Outcome.TERMINAL, status_code=422, error_code="permanent_rejection", error_class="PermanentRejection")
        return DownstreamResult(outcome=Outcome.TERMINAL, error_code="unknown_scenario", error_class="AdapterConfigurationError")
