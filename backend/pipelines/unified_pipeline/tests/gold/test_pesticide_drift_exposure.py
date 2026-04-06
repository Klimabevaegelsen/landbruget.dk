"""Tests for pesticide drift exposure gold processor.

Tests the Rautmann drift curve, wind direction weighting, meteorological
region assignment, and end-to-end drift dose calculations.
"""

import pytest

from unified_pipeline.gold.pesticide_drift_exposure import (
    MET_REGIONS,
    MIN_DISTANCE_M,
    RAUTMANN_A,
    RAUTMANN_B,
    _build_direction_frequency_table,
    _load_data_file,
    nearest_region_id,
    rautmann_drift_pct,
    wind_direction_weight,
)


class TestRautmannDriftFunction:
    """Verify the Rautmann power-law against JKI reference values."""

    @pytest.mark.parametrize(
        "distance,expected",
        [(1.0, 2.77), (5.0, 0.57), (10.0, 0.29), (20.0, 0.15), (50.0, 0.06)],
    )
    def test_matches_jki_table(self, distance: float, expected: float):
        result = rautmann_drift_pct(distance)
        assert abs(result - expected) < 0.02, (
            f"At {distance}m: got {result:.4f}, expected ~{expected}"
        )

    def test_monotonically_decreasing(self):
        distances = [1, 2, 5, 10, 20, 50, 100, 200, 500]
        values = [rautmann_drift_pct(d) for d in distances]
        for i in range(len(values) - 1):
            assert values[i] > values[i + 1], (
                f"Drift at {distances[i]}m ({values[i]:.4f}) should be > "
                f"drift at {distances[i + 1]}m ({values[i + 1]:.4f})"
            )

    def test_distance_clamp_at_zero(self):
        """Distance < 1m should be clamped to 1m."""
        assert rautmann_drift_pct(0.0) == rautmann_drift_pct(MIN_DISTANCE_M)
        assert rautmann_drift_pct(0.5) == rautmann_drift_pct(MIN_DISTANCE_M)
        assert rautmann_drift_pct(-1.0) == rautmann_drift_pct(MIN_DISTANCE_M)

    def test_returns_percentage(self):
        """Values should be in percent (0-100 range), not fraction."""
        assert 0 < rautmann_drift_pct(1.0) < 100
        assert 0 < rautmann_drift_pct(500.0) < 1

    def test_drift_at_500m_negligible(self):
        """At 500m, drift should be <0.02% (negligible)."""
        assert rautmann_drift_pct(500.0) < 0.02

    def test_formula_matches_coefficients(self):
        """Directly verify the power-law formula."""
        d = 10.0
        expected = RAUTMANN_A * (d**RAUTMANN_B)
        assert rautmann_drift_pct(d) == expected


class TestDriftDoseCalculation:
    """Test the dose calculation: dose_kg = DosageQuantity × AllocatedArea × drift_pct / 100."""

    def test_basic_dose(self):
        """Field at 10m, 5ha, 2.0 L/ha → dose = 2.0 × 5 × 0.29/100 ≈ 0.029 kg."""
        dosage = 2.0  # L/ha
        area = 5.0  # ha
        drift_pct = rautmann_drift_pct(10.0)  # ~0.29%
        dose_kg = dosage * area * drift_pct / 100.0
        assert abs(dose_kg - 0.029) < 0.005

    def test_dose_at_1m(self):
        """Closest possible field: high drift."""
        dosage = 1.0
        area = 1.0
        drift_pct = rautmann_drift_pct(1.0)
        dose_kg = dosage * area * drift_pct / 100.0
        assert abs(dose_kg - 0.0277) < 0.002

    def test_dose_scales_with_area(self):
        """Double the area → double the dose."""
        drift_pct = rautmann_drift_pct(20.0)
        dose_1ha = 1.0 * 1.0 * drift_pct / 100.0
        dose_2ha = 1.0 * 2.0 * drift_pct / 100.0
        assert abs(dose_2ha - 2 * dose_1ha) < 1e-10

    def test_mass_balance(self):
        """Drift dose must be << applied dose (sanity)."""
        dosage = 3.0  # L/ha
        area = 100.0  # ha
        applied_total = dosage * area  # 300 kg
        # Even at 1m, drift is only ~2.8% of applied dose per unit area
        drift_at_1m = dosage * area * rautmann_drift_pct(1.0) / 100.0
        assert drift_at_1m < applied_total * 0.05  # drift < 5% of total


class TestNearestRegionId:
    """Test nearest meteorological region assignment."""

    def test_aalborg_region(self):
        """Coordinates near Aalborg should map to region 2."""
        # Aalborg UTM: 555000, 6325000
        assert nearest_region_id(556000, 6326000) == 2

    def test_copenhagen_region(self):
        """Coordinates near Copenhagen should map to region 9."""
        # Copenhagen UTM: 725000, 6175000
        assert nearest_region_id(720000, 6170000) == 9

    def test_bornholm_region(self):
        """Coordinates near Bornholm should map to region 10."""
        # Bornholm UTM: 875000, 6115000
        assert nearest_region_id(870000, 6110000) == 10

    def test_thyboron_region(self):
        """Coordinates near Thyborøn should map to region 1."""
        assert nearest_region_id(460000, 6275000) == 1

    def test_all_regions_reachable(self):
        """Every region should be the nearest for its own coordinates."""
        for r in MET_REGIONS:
            assert nearest_region_id(r["utm_e"], r["utm_n"]) == r["id"]


class TestWindDirectionWeight:
    """Test wind direction frequency weighting."""

    def test_uniform_wind_rose(self):
        """With uniform wind in all directions, weight should be ~1/6 (6 bins out of 36)."""
        uniform_freq = [1.0 / 36.0] * 36
        weight = wind_direction_weight(180.0, uniform_freq)
        # With ~4-6 bins summed, expect roughly 4-6/36
        assert 0.05 < weight < 0.25

    def test_strong_westerly(self):
        """If wind is predominantly from west (270°), a building east of a field
        (bearing ~270° to field) should get high weight."""
        freq = [0.0] * 36
        # 270° = index 27
        freq[27] = 0.5
        freq[26] = 0.2
        freq[28] = 0.2
        # Bearing 270° means field is to the west, wind comes from west
        weight = wind_direction_weight(270.0, freq)
        assert weight > 0.5

    def test_opposite_direction_low(self):
        """Wind from west (270°), building west of field (bearing 90°) → low weight."""
        freq = [0.0] * 36
        freq[27] = 0.8  # Strong westerly
        weight = wind_direction_weight(90.0, freq)
        assert weight < 0.05

    def test_bearing_wraps_around(self):
        """Bearing of 355° should use bins near 0°/360°."""
        freq = [0.0] * 36
        freq[0] = 0.3  # 0° (= 360°)
        freq[35] = 0.3  # 350°
        freq[1] = 0.1  # 10°
        weight = wind_direction_weight(355.0, freq)
        assert weight > 0.3

    def test_zero_frequency_returns_zero(self):
        """If no wind at all (all zeros), weight should be 0."""
        zero_freq = [0.0] * 36
        assert wind_direction_weight(180.0, zero_freq) == 0.0


class TestWindRoseData:
    """Test wind rose data loading and structure."""

    def test_load_wind_rose_file(self):
        """Wind rose JSON should load and have expected structure."""
        data = _load_data_file("wind_rose_oml_2008-2017.json")
        assert "regions" in data
        assert len(data["regions"]) == 10

    def test_direction_frequency_table(self):
        """Direction frequency table should have 36 bins per region."""
        data = _load_data_file("wind_rose_oml_2008-2017.json")
        table = _build_direction_frequency_table(data)
        assert len(table) == 10
        for rid, freq in table.items():
            assert len(freq) == 36
            # Frequencies should be non-negative
            assert all(f >= 0 for f in freq)
            # Total frequency should sum to approximately 1.0
            total = sum(freq)
            assert 0.9 < total < 1.1, f"Region {rid}: total freq = {total}"

    def test_wind_rose_has_all_regions(self):
        """All 10 SR380 regions should be present."""
        data = _load_data_file("wind_rose_oml_2008-2017.json")
        table = _build_direction_frequency_table(data)
        for r in MET_REGIONS:
            assert r["id"] in table, f"Region {r['id']} ({r['name']}) missing from wind rose"


class TestSprayCalendar:
    """Test spray calendar data."""

    def test_load_spray_calendar(self):
        """Spray calendar should load successfully."""
        data = _load_data_file("spray_calendar.json")
        assert "winter_wheat" in data
        assert "_default" in data

    def test_probabilities_sum_to_one(self):
        """Each crop-pesticide monthly distribution should sum to 1.0."""
        data = _load_data_file("spray_calendar.json")
        for crop, types in data.items():
            if crop.startswith("_") and crop != "_default":
                continue
            if isinstance(types, str):
                continue
            for pest_type, months in types.items():
                if pest_type.startswith("_") and pest_type != "_default":
                    continue
                if isinstance(months, str):
                    continue
                total = sum(months.values())
                assert abs(total - 1.0) < 0.01, (
                    f"{crop}/{pest_type}: probabilities sum to {total}, expected 1.0"
                )

    def test_months_are_valid(self):
        """Month keys should be 1-12."""
        data = _load_data_file("spray_calendar.json")
        for crop, types in data.items():
            if crop.startswith("_") and crop != "_default":
                continue
            if isinstance(types, str):
                continue
            for pest_type, months in types.items():
                if pest_type.startswith("_") and pest_type != "_default":
                    continue
                if isinstance(months, str):
                    continue
                for month_str in months:
                    m = int(month_str)
                    assert 1 <= m <= 12, f"{crop}/{pest_type}: invalid month {m}"

    def test_winter_wheat_herbicide_bimodal(self):
        """Winter wheat herbicide should have autumn (Sep-Oct) and spring (Mar-May) peaks."""
        data = _load_data_file("spray_calendar.json")
        ww = data["winter_wheat"]["herbicide"]
        months = {int(m) for m in ww}
        # Must include autumn pre-emergence
        assert months & {9, 10}, "Should include autumn herbicide (Sep-Oct)"
        # Must include spring follow-up
        assert months & {3, 4, 5}, "Should include spring herbicide (Mar-May)"
        # April should be the single highest probability month
        assert max(ww, key=ww.get) == "4"

    def test_winter_rapeseed_herbicide_autumn(self):
        """Winter rapeseed herbicide includes autumn application (Sep-Oct)."""
        data = _load_data_file("spray_calendar.json")
        wr = data["winter_rapeseed"]["herbicide"]
        months = [int(m) for m in wr]
        assert 9 in months or 10 in months, "Rapeseed herbicide should include autumn"
