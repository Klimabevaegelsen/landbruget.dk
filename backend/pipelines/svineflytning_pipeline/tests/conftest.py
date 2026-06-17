"""Shared fixtures for svineflytning pipeline tests."""

import logging
import sys
from pathlib import Path

import pytest

# Ensure svineflytning_pipeline dir is first in sys.path so that
# `from main import main` resolves to this pipeline, not chr_pipeline.
_pipeline_dir = str(Path(__file__).resolve().parent.parent)
_pipeline_path = Path(_pipeline_dir)


def _is_local_test(item) -> bool:
    return Path(str(item.path)).resolve().is_relative_to(_pipeline_path)


def _is_sibling_pipeline_module(module: object) -> bool:
    path = getattr(module, "__file__", None)
    return path is not None and not Path(path).resolve().is_relative_to(_pipeline_path)


def _prefer_pipeline_imports() -> None:
    """Put this pipeline first and clear cached sibling pipeline modules."""
    for module_name, module in list(sys.modules.items()):
        if (
            module_name == "silver"
            or module_name.startswith("silver.")
            or module_name == "bronze"
            or module_name.startswith("bronze.")
        ) and _is_sibling_pipeline_module(module):
            del sys.modules[module_name]
    if _pipeline_dir in sys.path:
        sys.path.remove(_pipeline_dir)
    sys.path.insert(0, _pipeline_dir)


_prefer_pipeline_imports()


def pytest_runtest_setup(item):
    """Keep top-level bronze/silver imports pointed at svineflytning tests."""
    if _is_local_test(item):
        _prefer_pipeline_imports()


@pytest.fixture(autouse=True)
def suppress_logging():
    """Suppress verbose logging during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture()
def sample_bronze_chunk():
    """Minimal bronze chunk matching the SOAP response structure.

    This is the contract: bronze produces a list of these chunks,
    and silver expects to walk response → Response → SvineflytningListe → Svineflytning[].
    """
    return {
        "timestamp": "2026-03-01T00:00:00",
        "start_date": "2026-03-01",
        "end_date": "2026-03-03",
        "response": {
            "GLRCHRWSInfoOutbound": {},
            "Response": {
                "SvineflytningListe": {
                    "Svineflytning": [
                        {
                            "Id": 100001,
                            "Oprindelse": "CHR",
                            "Handling": "ret",
                            "FlytteTidspunkt": {
                                "SvineflytDato": "2026-03-01",
                                "SvineflytTidspunkt": 1,
                                "SvineflytRaekkefoelge": 1,
                            },
                            "Afsender": {
                                "Landekode": "DK",
                                "ChrNummer": 123456,
                                "BesaetningsNummer": 10001,
                                "Ejendom": {
                                    "Adresse": "Testvej 1",
                                    "ByNavn": "Testby",
                                    "PostNummer": 8000,
                                    "PostDistrikt": "Aarhus C",
                                    "KommuneNummer": 751,
                                    "KommuneNavn": "Aarhus",
                                    "DatoOpret": "2020-01-01",
                                    "DatoOpdatering": "2025-06-15",
                                },
                                "UdlandsEjendom": None,
                            },
                            "Modtager": {
                                "Landekode": "DK",
                                "ChrNummer": 654321,
                                "BesaetningsNummer": 20002,
                                "Ejendom": {
                                    "Adresse": "Modtagervej 2",
                                    "ByNavn": "Modtagerby",
                                    "PostNummer": 7100,
                                    "PostDistrikt": "Vejle",
                                    "KommuneNummer": 630,
                                    "KommuneNavn": "Vejle",
                                    "DatoOpret": "2019-05-10",
                                    "DatoOpdatering": "2024-12-01",
                                },
                                "UdlandsEjendom": None,
                            },
                            "AntalDyr": {
                                "AntalDyrIAlt": 150,
                                "AntalSoer": 0,
                                "AntalSlagtesvin": 150,
                                "Antal190LitersContainere": None,
                                "Antal240LitersContainere": None,
                            },
                            "Koeretoej": {
                                "Forvogn": {
                                    "Landekode": "DK",
                                    "RegNr": "AB12345",
                                },
                                "Haenger": {
                                    "Landekode": "DK",
                                    "RegNr": "CD67890",
                                },
                            },
                            "Omlaesser": None,
                            "TracesDokument": None,
                            "Sundhedscertifikat": None,
                            "IndberetterLogon": "testuser",
                            "IndberetningForetaget": "2026-03-01T10:00:00",
                        },
                        {
                            "Id": 100002,
                            "Oprindelse": "CHR",
                            "Handling": "slet",
                            "FlytteTidspunkt": None,
                            "Afsender": None,
                            "Modtager": None,
                            "AntalDyr": None,
                            "Koeretoej": None,
                            "Omlaesser": None,
                            "TracesDokument": None,
                            "Sundhedscertifikat": None,
                            "IndberetterLogon": None,
                            "IndberetningForetaget": None,
                        },
                    ]
                }
            },
        },
    }


@pytest.fixture()
def sample_bronze_data(sample_bronze_chunk):
    """A list of bronze chunks (as silver expects)."""
    return [sample_bronze_chunk]


@pytest.fixture()
def empty_bronze_chunk():
    """Bronze chunk with no movements — tests empty-data handling."""
    return {
        "timestamp": "2026-03-01T00:00:00",
        "start_date": "2026-03-01",
        "end_date": "2026-03-03",
        "response": {
            "GLRCHRWSInfoOutbound": {},
            "Response": {
                "SvineflytningListe": {},
            },
        },
    }
