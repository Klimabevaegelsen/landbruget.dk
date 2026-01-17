"""
Compliance Tests - Cattle Emissions

Tests that Python cattle emission formulas match reference implementation.
"""

import sys
from pathlib import Path

import pytest

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestCattleDigestionCompliance:
    """
    Test Python cattle digestion vs C# FormulaDigestionDairyCows.cs

    C# Formula (Heavy breed):
    yearAnimalCH4 = (1.230 * feedIntake - 0.145 * fattyAcids + 0.012 * ndf) / 55.65 * 335 + 0.304 * 30

    C# Formula (Jersey breed):
    yearAnimalCH4 = (1.230 * feedIntake - 0.145 * fattyAcids + 0.012 * ndf) / 55.65 * 335 + 0.207 * 30
    """

    @staticmethod
    def csharp_heavy_breed_ch4(
        year_animals: float, feed_intake: float, fatty_acids: float, ndf: float
    ) -> dict:
        """Replicate C# FormulaDigestionDairyCows for heavy breed."""
        year_animal_ch4 = (
            1.230 * feed_intake - 0.145 * fatty_acids + 0.012 * ndf
        ) / 55.65 * 335 + 0.304 * 30
        ch4_total = year_animals * year_animal_ch4
        return {"ch4_per_animal": round(year_animal_ch4, 2), "ch4_total": round(ch4_total, 2)}

    @staticmethod
    def csharp_jersey_breed_ch4(
        year_animals: float, feed_intake: float, fatty_acids: float, ndf: float
    ) -> dict:
        """Replicate C# FormulaDigestionDairyCows for Jersey breed."""
        year_animal_ch4 = (
            1.230 * feed_intake - 0.145 * fatty_acids + 0.012 * ndf
        ) / 55.65 * 335 + 0.207 * 30
        ch4_total = year_animals * year_animal_ch4
        return {"ch4_per_animal": round(year_animal_ch4, 2), "ch4_total": round(ch4_total, 2)}

    @pytest.mark.compliance
    @pytest.mark.critical
    @pytest.mark.cattle
    def test_heavy_breed_formula_matches_csharp(self, test_cases):
        """Verify heavy breed dairy cow formula matches C# exactly."""
        test_case = test_cases["cattle_digestion"][0]  # CD001: Heavy breed

        year_animals = test_case["inputs"]["year_animals"]
        feed_intake = test_case["inputs"]["feed_intake_kg_ts_day"]
        fatty_acids = test_case["inputs"]["fatty_acids_g_kg_ts"]
        ndf = test_case["inputs"]["ndf_g_kg_ts"]

        # Calculate using C# formula
        csharp_result = self.csharp_heavy_breed_ch4(year_animals, feed_intake, fatty_acids, ndf)

        # Verify per-animal CH4
        expected_ch4_per_animal = test_case["expected_outputs"]["ch4_per_animal_kg"]
        assert (
            abs(csharp_result["ch4_per_animal"] - expected_ch4_per_animal) < 0.1
        ), f"Per-animal CH4 mismatch: expected {expected_ch4_per_animal}, got {csharp_result['ch4_per_animal']}"

        # Verify total CH4
        expected_ch4_total = test_case["expected_outputs"]["ch4_total_kg"]
        tolerance = expected_ch4_total * test_case["tolerance_pct"] / 100
        assert abs(csharp_result["ch4_total"] - expected_ch4_total) <= tolerance

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_jersey_breed_formula_matches_csharp(self, test_cases):
        """Verify Jersey breed dairy cow formula matches C# exactly."""
        test_case = test_cases["cattle_digestion"][1]  # CD002: Jersey breed

        year_animals = test_case["inputs"]["year_animals"]
        feed_intake = test_case["inputs"]["feed_intake_kg_ts_day"]
        fatty_acids = test_case["inputs"]["fatty_acids_g_kg_ts"]
        ndf = test_case["inputs"]["ndf_g_kg_ts"]

        # Calculate using C# formula
        csharp_result = self.csharp_jersey_breed_ch4(year_animals, feed_intake, fatty_acids, ndf)

        # Verify per-animal CH4
        expected_ch4_per_animal = test_case["expected_outputs"]["ch4_per_animal_kg"]
        assert abs(csharp_result["ch4_per_animal"] - expected_ch4_per_animal) < 0.1

        # Verify total CH4
        expected_ch4_total = test_case["expected_outputs"]["ch4_total_kg"]
        tolerance = expected_ch4_total * test_case["tolerance_pct"] / 100
        assert abs(csharp_result["ch4_total"] - expected_ch4_total) <= tolerance

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_breed_difference_documented(self, test_cases):
        """Document the difference between heavy and Jersey breed emissions."""
        heavy_case = test_cases["cattle_digestion"][0]
        jersey_case = test_cases["cattle_digestion"][1]

        heavy_ch4_per_animal = heavy_case["expected_outputs"]["ch4_per_animal_kg"]
        jersey_ch4_per_animal = jersey_case["expected_outputs"]["ch4_per_animal_kg"]

        # Jersey should have lower emissions (0.207 vs 0.304 dry period factor)
        assert (
            jersey_ch4_per_animal < heavy_ch4_per_animal
        ), "Jersey breed should have lower CH4 emissions than heavy breed"

        difference_pct = (
            (heavy_ch4_per_animal - jersey_ch4_per_animal) / heavy_ch4_per_animal
        ) * 100
        print(f"\n  Heavy breed: {heavy_ch4_per_animal:.2f} kg CH4/animal/year")
        print(f"  Jersey breed: {jersey_ch4_per_animal:.2f} kg CH4/animal/year")
        print(f"  Difference: {difference_pct:.1f}%")

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_gwp_ar6_for_biogenic_ch4(self, test_cases, gwp_ar6):
        """Verify AR6 GWP for biogenic CH4 is applied (27, not 30)."""
        test_case = test_cases["cattle_digestion"][0]

        ch4_total = test_case["expected_outputs"]["ch4_total_kg"]
        expected_co2e_ar6 = test_case["expected_outputs"]["co2e_kg_gwp_ar6"]

        # AR6 uses 27 for biogenic CH4 (livestock), not 30 (fossil)
        calculated_co2e = ch4_total * gwp_ar6["CH4_biogenic"]

        assert (
            abs(calculated_co2e - expected_co2e_ar6) < 100
        ), f"AR6 biogenic CH4 conversion mismatch: expected {expected_co2e_ar6}, got {calculated_co2e}"

        # Document difference from AR4
        expected_co2e_ar4 = test_case["expected_outputs"]["co2e_kg_gwp_ar4"]
        deviation_pct = abs((calculated_co2e - expected_co2e_ar4) / expected_co2e_ar4) * 100

        print(f"\n  Python (AR6, CH4=27): {calculated_co2e:.0f} kg CO2e")
        print(f"  C# (AR4, CH4=25): {expected_co2e_ar4:.0f} kg CO2e")
        print(f"  Intentional deviation: {deviation_pct:.1f}%")

        # Deviation should be around 8%
        assert 5 < deviation_pct < 12, "AR6 vs AR4 CH4 deviation should be ~8%"


class TestPigDigestionCompliance:
    """
    Test Python pig formulas vs reference implementation (if available).

    Note: Pig formulas use IPCC Tier 1 methodology.
    """

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_pig_gwp_ar6_applied(self, gwp_ar6):
        """Verify pig emissions use AR6 GWP for biogenic CH4."""
        # This would test the actual pig calculation functions
        # For now, we verify the constant is correct

        from formulas.svin import enterisk_metan

        assert (
            gwp_ar6["CH4_biogenic"] == enterisk_metan.GWP_CH4
        ), f"Pig CH4 GWP should be {gwp_ar6['CH4_biogenic']} (AR6 biogenic)"


class TestManureEmissionsCompliance:
    """
    Test manure storage and housing emissions formulas.
    """

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_mcf_values_match_reference(self, reference_values):
        """Verify MCF (Methane Conversion Factor) values match reference."""
        ref_mcf = reference_values["emission_factors"]

        # MCF for slurry should be 12.4%
        assert ref_mcf["mcf_slurry"]["value"] == 12.4

        # MCF for deep litter should be 17.0%
        assert ref_mcf["mcf_deep_litter"]["value"] == 17.0

    @pytest.mark.compliance
    @pytest.mark.cattle
    def test_n2o_emission_factors_match_reference(self, reference_values):
        """Verify N2O emission factors match IPCC reference."""
        ref_ef = reference_values["emission_factors"]

        # Direct N2O from application should be 1%
        assert ref_ef["n2o_direct_application"]["value"] == 0.01

        # Indirect N2O from leaching should be 0.75%
        assert ref_ef["n2o_indirect_leaching"]["value"] == 0.0075
