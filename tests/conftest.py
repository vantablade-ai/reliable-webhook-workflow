import pytest

from app.core.clock import FakeClock
from app.repositories.workflow import WorkflowRepository


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def repo(tmp_path):
    return WorkflowRepository(str(tmp_path / "test.db"))
