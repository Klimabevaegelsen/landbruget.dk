"""Shared fixtures for the dyrenesdetektiv_scraper tests."""

import logging
import sys
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).resolve().parent.parent


def _clear_top_level_pipeline_modules() -> None:
    for cached in list(sys.modules):
        if (
            cached == "silver"
            or cached.startswith("silver.")
            or cached == "bronze"
            or cached.startswith("bronze.")
        ):
            del sys.modules[cached]


# Put this pipeline FIRST so its `silver`/`bronze` packages take precedence over
# any sibling pipeline (e.g. chr_pipeline) added to sys.path by backend/conftest.py.
while str(PIPELINE_DIR) in sys.path:
    sys.path.remove(str(PIPELINE_DIR))
sys.path.insert(0, str(PIPELINE_DIR))
# Drop any cached pipeline modules, including test doubles, that would shadow
# this pipeline during collection.
_clear_top_level_pipeline_modules()

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _is_local_test(item) -> bool:
    return Path(str(item.path)).resolve().is_relative_to(PIPELINE_DIR)


def pytest_runtest_setup(item):
    """Keep top-level bronze/silver imports pointed at dyrenesdetektiv tests."""
    if not _is_local_test(item):
        return
    while str(PIPELINE_DIR) in sys.path:
        sys.path.remove(str(PIPELINE_DIR))
    sys.path.insert(0, str(PIPELINE_DIR))
    _clear_top_level_pipeline_modules()


@pytest.fixture(autouse=True)
def suppress_logging():
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture()
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture()
def full_record_html() -> str:
    return (FIXTURES_DIR / "kontrol_full_chr_cvr.html").read_text(encoding="utf-8")


@pytest.fixture()
def slagteri_html() -> str:
    return (FIXTURES_DIR / "kontrol_slagteri_no_cvr.html").read_text(encoding="utf-8")


@pytest.fixture()
def hunde_html() -> str:
    return (FIXTURES_DIR / "kontrol_hunde.html").read_text(encoding="utf-8")
