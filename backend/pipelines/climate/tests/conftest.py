"""
Pytest configuration and fixtures for compliance testing.
"""

import pytest
import json
from pathlib import Path
from typing import Dict, Any


@pytest.fixture(scope="session")
def reference_values() -> Dict[str, Any]:
    """
    Load reference values for comparison.

    Returns:
        Dictionary containing GWP constants, emission factors, and other reference values
    """
    reference_path = Path(__file__).parent / "reference" / "reference_values.json"
    if reference_path.exists():
        with open(reference_path) as f:
            return json.load(f)
    return {}


@pytest.fixture(scope="session")
def test_cases() -> Dict[str, Any]:
    """
    Load test cases with expected inputs/outputs.

    Returns:
        Dictionary containing test cases organized by formula category
    """
    test_path = Path(__file__).parent / "reference" / "test_cases.json"
    if test_path.exists():
        with open(test_path) as f:
            return json.load(f)
    return {}


@pytest.fixture
def gwp_ar6() -> Dict[str, float]:
    """
    IPCC AR6 (2021) Global Warming Potentials (100-year).

    These are the most recent IPCC values and represent an intentional
    deviation from AR4 values.
    """
    return {
        "N2O": 273.0,           # IPCC AR6 (vs AR4: 298, AR5: 265)
        "CH4_biogenic": 27.0,   # Non-fossil CH4 (vs AR4: 25, AR5: 28)
        "CH4_fossil": 30.0,     # Fossil CH4 (vs AR4: 25, AR5: 28)
    }


@pytest.fixture
def gwp_ar4() -> Dict[str, float]:
    """
    IPCC AR4 Global Warming Potentials (100-year).

    These are the AR4 reference values.
    Used for testing formula parity with same constants.
    """
    return {
        "N2O": 298.0,
        "CH4": 25.0,
    }


def pytest_configure(config):
    """Add custom markers for test categorization."""
    config.addinivalue_line(
        "markers", "compliance: mark test as compliance verification test"
    )
    config.addinivalue_line(
        "markers", "critical: mark test as critical for reference parity"
    )
    config.addinivalue_line(
        "markers", "field: mark test as field emission formula test"
    )
    config.addinivalue_line(
        "markers", "cattle: mark test as cattle emission formula test"
    )
    config.addinivalue_line(
        "markers", "energy: mark test as energy emission formula test"
    )
