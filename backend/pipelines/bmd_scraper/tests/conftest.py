"""Shared fixtures for BMD scraper pipeline tests."""

import logging
import tempfile
from pathlib import Path

import duckdb
import pytest


@pytest.fixture(autouse=True)
def suppress_logging():
    """Suppress verbose logging during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture()
def temp_dir():
    """Provide a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture()
def bmd_raw_connection():
    """DuckDB connection with a `bmd_raw` table matching the bronze Excel schema.

    Contract: bronze produces an Excel file with these columns.
    Silver reads it with `read_xlsx(...)`. For schema contract tests we
    use an in-memory table to avoid xlsx extension dependency.
    """
    conn = duckdb.connect(":memory:")

    conn.execute("""
        CREATE TABLE bmd_raw AS
        SELECT * FROM (VALUES
            ('TestPesticid A', 'REG-001', NULL, NULL, NULL,
             'TestFirma A/S', 'Pesticid', 'Herbicid', NULL, 'Godkendt',
             'Almindelig', NULL, 'EC', 'Emulsionskoncentrat',
             'Herbicid', 'Diflufenican', 'CAS-123', '500 g/l', 'g/l',
             'Alle', NULL, 'Ukrudtsbekæmpelse i korn', NULL, NULL,
             '2020-01-15', '2030-12-31', NULL, NULL, NULL,
             NULL, NULL, NULL, NULL, NULL, NULL,
             'H302;H315', 'GHS07;GHS09', 'Advarsel', 'H302;H315',
             1.5, 2.0, 0.5, 4.0, 500.0, NULL),
            ('TestBiocid B', 'REG-002', 'UFI-001', 'EU-002', 'HandelB',
             'TestFirma B ApS', 'Biocid', NULL, 'Desinfektionsmiddel', 'Udgået',
             NULL, 'Biocid standard', 'SL', 'Opløsning',
             'Desinfektionsmiddel', 'Tebuconazol', 'CAS-456', '250 g/l', 'g/l',
             NULL, 'Professionelle', 'Overfladedesinfektion', NULL, NULL,
             '2018-06-01', '2025-03-31', '2025-06-30', '2025-09-30', NULL,
             NULL, NULL, NULL, NULL, NULL, NULL,
             'H318;H400', 'GHS05;GHS09', 'Fare', 'H318;H400',
             0.8, 1.2, 0.3, 2.3, 250.0, NULL)
        ) AS t(
            "Produktnavn", "Registrerings-nr.", "UFI-kode", "EU registrerings-nr.",
            "Yderligere handelsnavne",
            "Godkendelsesindehaver", "Bekæmpelsesmiddeltype", "Produktgruppe (pesticid)",
            "Produktgruppe (biocid)", "Produktstatus",
            "Godkendelsestype (pesticid)", "Godkendelsestype (biocid)",
            "Formulering", "Produktformuleringstype",
            "Aktivstoftype", "Aktivstofnavn(e)", "CAS-nr.", "Koncentration(er)", "Enhed(er)",
            "Bruger (pesticid)", "Bruger (biocid)", "Anvendelse",
            "Mindre anvendelse nr.", "Mindre anvendelse godkendelsesindehaver",
            "Godkendelsesdato", "Udløbsdato", "Godkendelses udløbsdato",
            "Frist for salg i detailled", "Frist for anvendelse og besiddelse",
            "Belastningsafgiftdato",
            "Risikosætninger", "Farebetegnelse (ild)", "Farebetegnelse (sundhed)",
            "Farebetegnelse (miljø)", "GHS-farepiktogrammer", "Signalord",
            "H-sætninger",
            "Belastning (miljøeffekt)", "Belastning (miljøadfærd)",
            "Belastning (sundhed)", "Samlet belastning",
            "Belastning koncentration", "Belastningsafgift"
        )
    """)

    yield conn
    conn.close()
