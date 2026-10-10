"""Fixtures shared by child receptors parser and validation tests."""

import sys
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
SOURCE_PACKAGE = Path(__file__).parent.parent / "src" / "child_receptors"
sys.path.insert(0, str(SOURCE_PACKAGE.parent))

# The monorepo's root pytest path imports this pipeline directory as the
# ``child_receptors`` package. Extend that package to the src-layout modules.
import child_receptors  # noqa: E402

if str(SOURCE_PACKAGE) not in child_receptors.__path__:
    child_receptors.__path__.append(str(SOURCE_PACKAGE))


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES
