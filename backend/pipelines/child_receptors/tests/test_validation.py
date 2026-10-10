"""Silver integrity and outlier checks."""

from __future__ import annotations

import pytest

from child_receptors.silver.build import validate_rows


def _row(receptor_id: str, *, east: float = 500000, cvr: str | None = None) -> dict:
    return {
        "receptor_id": receptor_id,
        "receptor_type": "daycare",
        "geometry": b"point",
        "utm_e": east,
        "utm_n": 6170000,
        "source": "dagtilbudsregister",
        "kommune_kode": "0101",
        "cvr": cvr,
        "p_number": None,
    }


def test_validation_duplicate_id_fails() -> None:
    with pytest.raises(ValueError, match="Duplicate receptor_id"):
        validate_rows([_row("dtr:1"), _row("dtr:1")], enforce_floors=False)


def test_validation_drops_bbox_outlier_below_per_source_threshold() -> None:
    rows = [_row(f"dtr:{index}") for index in range(200)] + [_row("dtr:outside", east=950000)]
    qa = {"dropped_rows": {}, "warnings": {}}
    valid = validate_rows(rows, qa=qa, enforce_floors=False)
    assert len(valid) == 200
    assert qa["dropped_rows"]["outside_denmark_bbox:dagtilbudsregister"] == 1


def test_validation_fails_when_more_than_half_percent_of_a_source_is_outside() -> None:
    rows = [_row(f"dtr:{index}") for index in range(198)] + [_row("dtr:outside", east=950000)]
    with pytest.raises(ValueError, match=r">0\.5%"):
        validate_rows(rows, enforce_floors=False)


def test_validation_cvr_must_be_eight_digits() -> None:
    with pytest.raises(ValueError, match="Invalid CVR format"):
        validate_rows([_row("dtr:1", cvr="123")], enforce_floors=False)


def test_validation_floors_accept_small_test_override() -> None:
    floors = {
        "daycare": 1,
        "school": 0,
        "playground": 0,
        "daycare_kommune_count": 1,
    }
    assert len(validate_rows([_row("dtr:1")], minimums=floors)) == 1
