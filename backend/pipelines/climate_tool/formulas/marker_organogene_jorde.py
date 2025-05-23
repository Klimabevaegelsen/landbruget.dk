"""
Beregning af CO2 fra organogene jorde
"""

from typing import Tuple, Any
import json
from pathlib import Path

# Utility function to load data from JSON files
if 'load_json_data' not in globals():
    def load_json_data(file_path: str) -> Any:
        base_path = Path(__file__).resolve().parent.parent / "reference_values"
        full_path = base_path / file_path
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Error: JSON file not found at {full_path}")
            raise
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from {full_path}")
            raise

# Load data from tables
try:
    tabel_31_data = load_json_data("tabel_31_emission_af_co2_fra_nedbrydning_af_organisk_stof_på_organogen_jord_ton_co2_pr_ha_side_96.json")
    co2_kulstof_emission_factors = {}
    for row in tabel_31_data.get('data', []):
        c_content = row.get("C_Content")
        if c_content not in co2_kulstof_emission_factors:
            co2_kulstof_emission_factors[c_content] = {}
        # Ensure keys from table match expected keys "Omdrift" and "Permanent_græs_og_afvandet"
        co2_kulstof_emission_factors[c_content]["Omdrift"] = row.get("Omdrift")
        co2_kulstof_emission_factors[c_content]["Permanent_græs_og_afvandet"] = row.get("Permanent_græs_og_afvandet")
except Exception as e:
    print(f"Failed to load or parse tabel_31_data: {e}")
    co2_kulstof_emission_factors = None

try:
    tabel_32_data = load_json_data("tabel_32_effekter_af_udtagning_af_organogen_jord_olesen_et_al_2018_dca_rapport_nr_130_side_97.json")
    ch4_co2e_factor_hoj_vandstand = None
    for row in tabel_32_data.get('data', []):
        # Matching based on the description for rule 5c which is for "Vådområde (ikke dyrket)"
        # The python code implies this factor is the same regardless of C_Content for "høj vandstand"
        if row.get("Forvaltningstiltag") == "Vådområde (ikke dyrket)":
            ch4_co2e_factor_hoj_vandstand = row.get("CH4_CO2e_kg_ha_aar") / 1000.0  # Convert kg to tons
            break
except Exception as e:
    print(f"Failed to load or parse tabel_32_data: {e}")
    ch4_co2e_factor_hoj_vandstand = None

# Hardcoded N2O CO2e values (tons CO2e from N2O), as their derivation from tables is unclear
N2O_CO2E_OMDRIFT_GT12C = 3.87
N2O_CO2E_PERMGRAS_GT12C = 2.44

# Define constants for CO2e conversion if needed, though the direct values are CO2 tons from the source
# N2O_TO_CO2E_FACTOR = 265.0 # Stated in the formula doc for N2O to CO2e conversion
# CH4_TO_CO2E_FACTOR = 25.0 # Stated in the formula doc for CH4 to CO2e conversion (Note: common IPCC GWP100 for CH4 is 28 for AR5, 25 is older or specific context)
# The formulas in the notebook provide results directly in "tons CO2" or "tons CO2e for N2O/CH4", so direct use of these factors might not be needed if inputs are already converted.
# The problem states: "I resultatkolonnen skal resultatet vises i CO2e", and then sums CO2_kulstof, CO2_N2O, CO2_CH4.
# This implies the individual components (1a, 1b, 1c etc.) are already in the final CO2e/CO2 tons for that component.

def calculate_co2_organogene_jorde(
    h: float,
    i_omdrift: bool,
    lav_vandstand: bool,
    kulstof_percentage: str,  # Expected "6-12%" or ">12%"
) -> Tuple[float, float, float, float]:
    """
    Calculates CO2 emissions from organogene jorde based on land characteristics.

    Args:
        h (float): Antal ha marken.
        i_omdrift (bool): True if marken er "i omdrift", False otherwise.
        lav_vandstand (bool): True if marken har "lav vandstand", False if "høj vandstand".
        kulstof_percentage (str): Kulstofindhold, "6-12%C" or ">12%C" (matching table keys).

    Returns:
        Tuple[float, float, float, float]:
            - co2_tons_kulstof (float): Samlet tons CO2 fra kulstof.
            - co2_tons_n2o (float): Samlet tons CO2e fra N2O.
            - co2_tons_ch4 (float): Samlet tons CO2e fra CH4.
            - co2_tons_total (float): Samlet tons CO2e total.
    """
    co2_tons_kulstof = 0.0
    co2_tons_n2o = 0.0
    co2_tons_ch4 = 0.0

    # Use a string that matches keys in co2_kulstof_emission_factors for kulstof_percentage
    # The table uses "6-12%C" and ">12%C"
    # The input to this function was defined as "6-12%" or ">12%"
    # We should ensure consistency or map the input.
    # For now, assume input kulstof_percentage will be adjusted or directly match table keys if necessary.
    # Let's assume the input `kulstof_percentage` is already in the format "6-12%C" or ">12%C"
    # or that a mapping step occurs before calling this, or we add it here:
    mapped_kulstof_percentage = kulstof_percentage # Default if already correct
    if kulstof_percentage == "6-12%":
        mapped_kulstof_percentage = "6-12%C"
    elif kulstof_percentage == ">12%":
        mapped_kulstof_percentage = ">12%C"

    fallback_to_hardcoded = False
    if co2_kulstof_emission_factors is None or ch4_co2e_factor_hoj_vandstand is None:
        print("Warning: Table data for organogene jorde not fully loaded. Falling back to hardcoded values where necessary.")
        fallback_to_hardcoded = True

    if i_omdrift and lav_vandstand:
        ef_kulstof_key = "Omdrift"
        if mapped_kulstof_percentage == "6-12%C":
            # Regel 1
            co2_tons_kulstof = h * (co2_kulstof_emission_factors.get(mapped_kulstof_percentage, {}).get(ef_kulstof_key, 21.08) if not fallback_to_hardcoded else 21.08)
            # co2_tons_n2o = 0.0 (as per original)
            # co2_tons_ch4 = 0.0 (as per original)
        elif mapped_kulstof_percentage == ">12%C":
            # Regel 2
            co2_tons_kulstof = h * (co2_kulstof_emission_factors.get(mapped_kulstof_percentage, {}).get(ef_kulstof_key, 42.17) if not fallback_to_hardcoded else 42.17)
            co2_tons_n2o = h * N2O_CO2E_OMDRIFT_GT12C # Using the hardcoded N2O factor: 3.87
            # co2_tons_ch4 = 0.0 (as per original)
        else:
            raise ValueError(f"Invalid kulstof_percentage: {kulstof_percentage} for i_omdrift and lav_vandstand")

    elif not i_omdrift and lav_vandstand:
        ef_kulstof_key = "Permanent_græs_og_afvandet"
        if mapped_kulstof_percentage == ">12%C":
            # Regel 3
            co2_tons_kulstof = h * (co2_kulstof_emission_factors.get(mapped_kulstof_percentage, {}).get(ef_kulstof_key, 30.8) if not fallback_to_hardcoded else 30.8)
            co2_tons_n2o = h * N2O_CO2E_PERMGRAS_GT12C # Using the hardcoded N2O factor: 2.44
            # co2_tons_ch4 = 0.0 (as per original)
        elif mapped_kulstof_percentage == "6-12%C":
            # Regel 4
            co2_tons_kulstof = h * (co2_kulstof_emission_factors.get(mapped_kulstof_percentage, {}).get(ef_kulstof_key, 15.4) if not fallback_to_hardcoded else 15.4)
            # co2_tons_n2o = 0.0 (as per original)
            # co2_tons_ch4 = 0.0 (as per original)
        else:
            raise ValueError(f"Invalid kulstof_percentage: {kulstof_percentage} for not i_omdrift and lav_vandstand")

    elif not i_omdrift and not lav_vandstand:  # Høj vandstand
        # Regel 5 - Kulstofindhold betyder ingenting for kulstof and N2O (both are 0)
        # co2_tons_kulstof = 0.0 (as per original)
        # co2_tons_n2o = 0.0 (as per original)
        co2_tons_ch4 = h * (ch4_co2e_factor_hoj_vandstand if not fallback_to_hardcoded and ch4_co2e_factor_hoj_vandstand is not None else 6.8) # Loaded (should be 6.8)
    else:
        # This case (i_omdrift and not lav_vandstand (høj vandstand)) is not explicitly covered.
        pass

    co2_tons_total = co2_tons_kulstof + co2_tons_n2o + co2_tons_ch4
    return co2_tons_kulstof, co2_tons_n2o, co2_tons_ch4, co2_tons_total


# Testcases from Marker/Organogene_jorde.ipynb
if __name__ == "__main__":
    H_test = 10.0

    # Test 1: Marker i omdrift og med lav vandstand (6-12% kulstof)
    kulstof1, n2o1, ch41, total1 = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=True, lav_vandstand=True, kulstof_percentage="6-12%"
    )
    print(f"Test 1 (i omdrift, lav vand, 6-12% C): Kulstof={kulstof1:.2f}t, N2O={n2o1:.2f}t, CH4={ch41:.2f}t, Total={total1:.2f}t CO2e")
    # Expected total: 10.0 * 21.08 = 210.8

    # Test 2: Marker i omdrift og med lav vandstand (>12% kulstof)
    kulstof2, n2o2, ch42, total2 = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=True, lav_vandstand=True, kulstof_percentage=">12%"
    )
    print(f"Test 2 (i omdrift, lav vand, >12% C): Kulstof={kulstof2:.2f}t, N2O={n2o2:.2f}t, CH4={ch42:.2f}t, Total={total2:.2f}t CO2e")
    # Expected total: 10 * 42.17 (kulstof) + 10 * 3.87 (N2O) = 421.7 + 38.7 = 460.4

    # Test 3: Marker ikke i omdrift og med lav vandstand (>12% kulstof)
    kulstof3, n2o3, ch43, total3 = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=False, lav_vandstand=True, kulstof_percentage=">12%"
    )
    print(f"Test 3 (ikke omdrift, lav vand, >12% C): Kulstof={kulstof3:.2f}t, N2O={n2o3:.2f}t, CH4={ch43:.2f}t, Total={total3:.2f}t CO2e")
    # Expected total: 10 * 30.8 (kulstof) + 10 * 2.44 (N2O) = 308.0 + 24.4 = 332.4

    # Test 4: Marker ikke i omdrift og med lav vandstand (6-12% kulstof)
    kulstof4, n2o4, ch44, total4 = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=False, lav_vandstand=True, kulstof_percentage="6-12%"
    )
    print(f"Test 4 (ikke omdrift, lav vand, 6-12% C): Kulstof={kulstof4:.2f}t, N2O={n2o4:.2f}t, CH4={ch44:.2f}t, Total={total4:.2f}t CO2e")
    # Expected total: 10 * 15.4 = 154.0

    # Test 5: Marker ikke i omdrift og med høj vandstand (kulstofindhold irrelevant)
    # Testing with both kulstof percentages to show it doesn't matter for this rule.
    kulstof5a, n2o5a, ch45a, total5a = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=False, lav_vandstand=False, kulstof_percentage="6-12%"
    )
    print(f"Test 5a (ikke omdrift, høj vand, 6-12% C): Kulstof={kulstof5a:.2f}t, N2O={n2o5a:.2f}t, CH4={ch45a:.2f}t, Total={total5a:.2f}t CO2e")
    # Expected total: 10 * 6.8 (CH4) = 68.0

    kulstof5b, n2o5b, ch45b, total5b = calculate_co2_organogene_jorde(
        h=H_test, i_omdrift=False, lav_vandstand=False, kulstof_percentage=">12%"
    )
    print(f"Test 5b (ikke omdrift, høj vand, >12% C): Kulstof={kulstof5b:.2f}t, N2O={n2o5b:.2f}t, CH4={ch45b:.2f}t, Total={total5b:.2f}t CO2e")
    # Expected total: 10 * 6.8 (CH4) = 68.0

    # Test undefined case explicitly (i_omdrift=True, lav_vandstand=False)
    print("\nTesting undefined case (i omdrift, høj vandstand):")
    try:
        kulstof_undef, n2o_undef, ch4_undef, total_undef = calculate_co2_organogene_jorde(
            h=H_test, i_omdrift=True, lav_vandstand=False, kulstof_percentage="6-12%"
        )
        print(f"  Result: Kulstof={kulstof_undef:.2f}t, N2O={n2o_undef:.2f}t, CH4={ch4_undef:.2f}t, Total={total_undef:.2f}t CO2e (Should be 0 or error based on current logic)")
    except ValueError as e:
        print(f"  Caught expected error for undefined case: {e}")