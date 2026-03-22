"""Shared fixtures for DMA scraper pipeline tests."""

import logging
import sys
from pathlib import Path

import pytest

# Ensure dma_scraper pipeline dir is first in sys.path so that
# ``from main import main`` resolves to dma_scraper/main.py
# (not chr_pipeline/main.py which is added by the root conftest).
_dma_dir = str(Path(__file__).resolve().parent.parent)


def pytest_configure(config):
    """Ensure dma_scraper dir is first in sys.path before test collection."""
    # Remove any cached 'main' module from other pipelines
    if "main" in sys.modules:
        del sys.modules["main"]
    # Put dma_scraper dir at the front of sys.path
    if _dma_dir in sys.path:
        sys.path.remove(_dma_dir)
    sys.path.insert(0, _dma_dir)


@pytest.fixture(autouse=True)
def suppress_logging():
    """Suppress verbose logging during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture()
def sample_bronze_records():
    """Minimal bronze JSON records matching the DMA scraper output.

    Contract: bronze produces a list of dicts, each with company fields
    plus three nested arrays: Tilsyn, Håndhævelser, Afgørelser.
    Silver expects to flatten these via UNNEST.
    """
    return [
        {
            "miljoeaktoerUrl": "/Miljoeaktoer/100001",
            "myndighedUrl": "/Myndighed/751",
            "navn": "TestFarm ApS",
            "cvr_number": "12345678",
            "chr": "123456",
            "pnr": "1001234567",
            "mstNoegle": "MST-001",
            "fuldAdresse": "Testvej 1, 8000 Aarhus",
            "bynavn": "Aarhus",
            "postNummer": "8000",
            "vejnavn": "Testvej",
            "husNummer": "1",
            "hovedaktivitetKode": "01.11",
            "hovedaktivitetTekst": "Dyrkning af korn",
            "miljoeaktoerGruppeKode": "IED",
            "miljoeaktoerGruppeTekst": "IED-virksomhed",
            "godkendelsespligtig": True,
            "risikovirksomhed": False,
            "stedfaestelse": "55.6761,12.5683",
            "ansvarligMyndighed": "Aarhus Kommune",
            "ansvarligMyndighedKnr": "751",
            "Tilsyn": [
                {
                    "date": "2025-06-15",
                    "cvr_number": "12345678",
                    "pdf_url": "/tilsyn/doc1.pdf",
                    "result": "Ingen anmærkninger",
                },
            ],
            "Håndhævelser": [
                {
                    "action": "Indskærpelse",
                    "date": "2024-11-20",
                    "cvr_number": "12345678",
                },
            ],
            "Afgørelser": [
                {
                    "decision": "Miljøgodkendelse",
                    "date": "2023-03-10",
                    "pdf_url": "/afgoerelse/doc2.pdf",
                },
            ],
        },
        {
            "miljoeaktoerUrl": "/Miljoeaktoer/100002",
            "myndighedUrl": "/Myndighed/630",
            "navn": "EmptyFarm A/S",
            "cvr_number": "87654321",
            "chr": "",
            "pnr": "",
            "mstNoegle": "MST-002",
            "fuldAdresse": "Tomvej 99, 7100 Vejle",
            "bynavn": "Vejle",
            "postNummer": "7100",
            "vejnavn": "Tomvej",
            "husNummer": "99",
            "hovedaktivitetKode": "01.50",
            "hovedaktivitetTekst": "Blandet drift",
            "miljoeaktoerGruppeKode": "",
            "miljoeaktoerGruppeTekst": "",
            "godkendelsespligtig": False,
            "risikovirksomhed": False,
            "stedfaestelse": "",
            "ansvarligMyndighed": "Vejle Kommune",
            "ansvarligMyndighedKnr": "630",
            "Tilsyn": [],
            "Håndhævelser": [],
            "Afgørelser": [],
        },
    ]


@pytest.fixture()
def sample_bronze_record_with_empty_keys():
    """Bronze record where nested arrays contain empty-string keys.

    Silver must sanitize these (strip entries with blank keys).
    """
    return [
        {
            "miljoeaktoerUrl": "/Miljoeaktoer/100003",
            "navn": "DirtyData Gård",
            "cvr_number": "11223344",
            "Tilsyn": [
                {
                    "date": "2025-01-01",
                    "": "should-be-removed",
                    "  ": "also-removed",
                    "result": "OK",
                },
            ],
            "Håndhævelser": [],
            "Afgørelser": [],
        }
    ]
