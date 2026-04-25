"""Shared fixtures for the dyrenesdetektiv_scraper tests."""

import logging
import sys
from pathlib import Path

import pytest

PIPELINE_DIR = Path(__file__).resolve().parent.parent
# Put this pipeline FIRST so its `silver`/`bronze` packages take precedence over
# any sibling pipeline (e.g. chr_pipeline) added to sys.path by backend/conftest.py.
while str(PIPELINE_DIR) in sys.path:
    sys.path.remove(str(PIPELINE_DIR))
sys.path.insert(0, str(PIPELINE_DIR))
# Drop any cached sibling-pipeline modules that would shadow ours.
for cached in [m for m in list(sys.modules) if m == "silver" or m.startswith("silver.")]:
    del sys.modules[cached]
for cached in [m for m in list(sys.modules) if m == "bronze" or m.startswith("bronze.")]:
    del sys.modules[cached]

FIXTURES_DIR = Path(__file__).parent / "fixtures"


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
