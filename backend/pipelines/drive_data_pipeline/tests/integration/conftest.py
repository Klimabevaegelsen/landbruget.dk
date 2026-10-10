"""Fixtures shared by the hermetic integration tests."""

from unittest.mock import MagicMock

import common.storage
import pytest


@pytest.fixture(autouse=True)
def mock_storage_access(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Keep integration tests from constructing a real R2 storage client."""
    storage_access = MagicMock()
    storage_access_type = MagicMock(return_value=storage_access)
    monkeypatch.setattr(common.storage, "StorageAccess", storage_access_type)
    return storage_access
