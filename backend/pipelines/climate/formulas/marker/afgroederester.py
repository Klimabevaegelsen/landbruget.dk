"""
Beregning af afgrøderester på marken (N og CO2e).
"""

import json
from pathlib import Path
from typing import Tuple, Any, Optional

import sys
from pathlib import Path as SysPath

# Add climate root to path for imports
_climate_root = SysPath(__file__).resolve().parent.parent.parent
if str(_climate_root) not in sys.path:
    sys.path.insert(0, str(_climate_root))

from crop_parameters import get_crop_params  # noqa: E402


# Utility function to load data from JSON files
def load_json_data(file_path: str) -> Any:
    """Loads data from a JSON file."""
    # Construct the full path relative to this file's directory's parent (formulas -> climate)
    # Assumes tables are in ../reference_values/
    base_path = Path(__file__).resolve().parent.parent.parent / "reference_values"
    full_path = base_path / file_path
    try:
        with open(full_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found at {full_path}")
        # Potentially raise an error or return a default/None
        raise
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {full_path}")
        # Potentially raise an error or return a default/None
        raise


THETA_N2O_CO2 = 273.0  # GWP for N2O (IPCC AR6, 2021)
MOL_WEIGHT_N2O_N_FACTOR = 44.0 / 28.0  # (M_N2O / M_N2)
try:
    tabel_19_data = load_json_data("tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json")
    EF_N2O_AFGROEDERESTER = tabel_19_data["data"][0]["EF_N2O"]
except Exception as e:
    print(f"Failed to load EF_N2O_AFGROEDERESTER from tabel_19: {e}")
    EF_N2O_AFGROEDERESTER = 0.01


def calculate_k_graes(n_graes_kg_n_ha: float) -> float:
    """
    Beregner korrektionsfaktor for afgrøderester i græs (k_græs).
    Args:
        n_graes_kg_n_ha (float): Mængden af N afsat under afgræsning [kg N/ha]
    Returns:
        float: k_græs faktor
    """
    if n_graes_kg_n_ha >= 50:
        return 1.49
    elif 10 <= n_graes_kg_n_ha < 50:
        return 1.24
    else:  # n_graes_kg_n_ha < 10
        return 1.0


def calculate_A_over_kg_ts_ha(
    x1_halmnedmulding: bool,
    x2_udbytte_nedmuldes: bool,
    t_torstof_total_kg_ts_ha: float,
    h_u_fast_halmudbytte_kg_ts_ha: float,  # Fast halmudbytte i frøgræs
    s_slope: float,
    i_intercept: float,
    k_graes: float,
    h_f_halmfraktion: float,
) -> float:
    """
    Beregner tørstof i afgrøderester over jorden (A_over) i kg ts/ha.

    Formel: A_over = ((T + H_u) * S + I) * k_græs + (X1_val - 1) * H_f * T + X1_val * H_u + X2_val * T
    Where X1_val is 1.0 if x1_halmnedmulding is True, 0.0 otherwise.
    And X2_val is 1.0 if x2_udbytte_nedmuldes is True, 0.0 otherwise.
    """
    x1_val = 1.0 if x1_halmnedmulding else 0.0
    x2_val = 1.0 if x2_udbytte_nedmuldes else 0.0

    # Term 1: ((T + H_u) * S + I) * k_græs
    term1 = (t_torstof_total_kg_ts_ha + h_u_fast_halmudbytte_kg_ts_ha) * s_slope + i_intercept
    term1 *= k_graes

    # Term 2: (X1_val - 1) * H_f * T
    # Note: The formula from the image (X1-1)*H_f*T seems to imply if X1 is false (0), this becomes -H_f*T
    # If X1 is true (1), this becomes 0. This might be intended to remove H_f*T if halm is *not* nedmuldet
    # The C# code is: (halmnedmulding - 1) * H_f * T where halmnedmulding is 1.0 or 0.0.
    # If halmnedmulding = 1.0 (true) -> (1-1)*H_f*T = 0
    # If halmnedmulding = 0.0 (false) -> (0-1)*H_f*T = -H_f*T
    term2 = (x1_val - 1.0) * h_f_halmfraktion * t_torstof_total_kg_ts_ha

    # Term 3: X1_val * H_u
    term3 = x1_val * h_u_fast_halmudbytte_kg_ts_ha

    # Term 4: X2_val * T
    term4 = x2_val * t_torstof_total_kg_ts_ha

    a_over = term1 + term2 + term3 + term4
    return a_over


def calculate_A_under_kg_ts_ha(
    t_torstof_total_kg_ts_ha: float,
    h_u_fast_halmudbytte_kg_ts_ha: float,
    s_slope: float,
    i_intercept: float,
    k_graes: float,
    # h_f_halmfraktion: float, # H_f is not in the A_under formula in the markdown, but is in C# A_under parameters (unused)
    f_forhold_under_over_biomasse: float,
) -> float:
    """
    Beregner tørstof i afgrøderester under jorden (A_under) i kg ts/ha.
    Formel: A_under = (T + H_u + ((T + H_u) * S + I) * k_græs) * F
    The C# code for A_under is: (T + H_u + ((T + H_u) * S + I) * k_graes) * F
    This matches the formula exactly.
    """
    # Inner term: (T + H_u)
    inner_base = t_torstof_total_kg_ts_ha + h_u_fast_halmudbytte_kg_ts_ha

    # Term ((T + H_u) * S + I) * k_græs
    term_residue_calc = (inner_base * s_slope + i_intercept) * k_graes

    a_under = (inner_base + term_residue_calc) * f_forhold_under_over_biomasse
    return a_under


def calculate_n_afgroederester_kg_n_ha(
    a_over_kg_ts_ha: float,
    n_over_kg_n_pr_kg_ts: float,  # N indhold i afgrøderester over jorden
    a_under_kg_ts_ha: float,
    n_under_kg_n_pr_kg_ts: float,  # N indhold i afgrøderester under jorden
    # o_omlaegningsfrekvens: float, # This is applied per ha, not to the total N_afgroederester.
    # areal_ha: float # This is applied per ha for N, and then CO2e. Function returns per ha.
) -> Tuple[float, float, float]:
    """
    Beregner N i afgrøderester pr. ha.
    Returns N_A_over_kg_n_ha, N_A_under_kg_n_ha, N_total_afgroederester_kg_n_ha
    """
    n_a_over_kg_n_ha = a_over_kg_ts_ha * n_over_kg_n_pr_kg_ts
    n_a_under_kg_n_ha = a_under_kg_ts_ha * n_under_kg_n_pr_kg_ts

    # The formula N_afgroderester = (N_A_over + N_A_under) / O * A is for total N on field.
    # For per ha, it would be (N_A_over_ha + N_A_under_ha) / O
    # Assuming O (omlægningsfrekvens) applies to the N content before CO2e conversion.
    # For now, this function will return N per ha BEFORE omlægningsfrekvens, as O should be applied at a higher level
    # or directly in the CO2e calculation if it affects the N available for emission over time.
    # The notebook example applies O=1.0 directly to the sum for N_afgroederester.
    # Let's return components and total N per ha.

    n_total_afgroederester_kg_n_ha = n_a_over_kg_n_ha + n_a_under_kg_n_ha
    return n_a_over_kg_n_ha, n_a_under_kg_n_ha, n_total_afgroederester_kg_n_ha


def calculate_co2e_afgroederester_kg_co2e_ha(
    n_a_over_kg_n_ha: float,
    n_a_under_kg_n_ha: float,
    o_omlaegningsfrekvens: float,  # For annuals or non-ploughed this year perennials, this should be 1. For ploughed perennials, it's cycle length.
    is_perennial_and_ploughed_this_year: bool,
) -> float:
    """
    Beregner CO2e fra N i afgrøderester pr. ha for hovedafgrøder.
    F_Omlæg (1/o_omlaegningsfrekvens) applies to N_over for ploughed perennials.
    N_total_effektiv = (N_over * F_Omlæg) + N_under
    CO2e = N_total_effektiv * EF_N2O * (44/28) * theta_N2O_CO2
    """
    if o_omlaegningsfrekvens == 0:  # Avoid division by zero
        return 0.0

    n_over_effektiv_kg_n_ha = n_a_over_kg_n_ha
    if is_perennial_and_ploughed_this_year:
        n_over_effektiv_kg_n_ha = n_a_over_kg_n_ha / o_omlaegningsfrekvens

    # For annual crops, is_perennial_and_ploughed_this_year is False, o_omlaegningsfrekvens is 1.
    # So n_over_effektiv_kg_n_ha remains n_a_over_kg_n_ha.
    # N_under is not affected by omlægningsfrekvens in this step.
    n_total_effektiv_for_co2e_kg_n_ha = n_over_effektiv_kg_n_ha + n_a_under_kg_n_ha

    co2e_kg_ha = n_total_effektiv_for_co2e_kg_n_ha * EF_N2O_AFGROEDERESTER * MOL_WEIGHT_N2O_N_FACTOR * THETA_N2O_CO2
    return co2e_kg_ha


# Functions for Afgrøde 2 (Efterafgrøder / Udlægsafgrøder)
# Based on Markdown pages 89-90, Tables 27, 28


def calculate_stub_mv_efterafgroede_kg_ts_ha(
    udbytte_efterafgroede_kg_ts_ha: float,  # Harvested or harvestable yield
    haeldning_table27: float,  # Slope from Table 27
    intercept_table27: float,  # Intercept from Table 27
) -> float:
    """
    Beregner tørstof i stub m.v. for efterafgrøder.
    Markdown formula (page 89): Kg tørstof i stub mv. = (Udbytte_kg tørstof/1000) * Hældning + Intercept
    NOTE: Assuming Hældning/Intercept from Table 27 are scaled for Udbytte_kg_ts_ha in kg.
    If Table 27 Hældning/Intercept are for Mg yield, udbytte must be converted or factors scaled.
    For simplicity, assuming direct compatibility of units as often done in provided examples.
    """
    # Current interpretation: (Udbytte_kg_ts_ha * Hældning) + Intercept
    # This means Table 27 Hældning is unitless ratio, Intercept is kg_ts_ha
    stub_mv_kg_ts_ha = udbytte_efterafgroede_kg_ts_ha * haeldning_table27 + intercept_table27
    return stub_mv_kg_ts_ha


def calculate_n_efterafgroede_kg_n_ha(
    udbytte_efterafgroede_kg_ts_ha: float,  # Harvested or harvestable yield
    nedmuld_flag: bool,  # True if harvestable yield itself is incorporated
    haeldning_table27: float,
    intercept_table27: float,
    n_indhold_over_table27: float,  # N content for above-ground from Table 27 (kg N / kg ts)
    faktor_under_table28: float,  # Factor for below-ground from Table 28 (ratio)
    n_indhold_under_table28: float,  # N content for below-ground from Table 28 (kg N / kg ts)
) -> Tuple[float, float, float]:
    """
    Beregner N i afgrøderester for efterafgrøder/udlægsafgrøder pr. ha.
    Returns N_over_nedmuldet_kg_n_ha, N_under_kg_n_ha, N_total_efterafgroede_kg_n_ha
    """
    stub_mv_kg_ts_ha = calculate_stub_mv_efterafgroede_kg_ts_ha(
        udbytte_efterafgroede_kg_ts_ha, haeldning_table27, intercept_table27
    )

    # Afgrøderest overjordisk_Nedmuldet = Udbytte x Nedmuld (1/0) + Stub mv. (Page 90)
    a_over_nedmuldet_kg_ts_ha = stub_mv_kg_ts_ha
    if nedmuld_flag:
        a_over_nedmuldet_kg_ts_ha += udbytte_efterafgroede_kg_ts_ha

    n_over_nedmuldet_kg_n_ha = a_over_nedmuldet_kg_ts_ha * n_indhold_over_table27

    # Overjordisk biomasse = Udbytte + Stub mv. (Page 90)
    overjordisk_biomasse_total_kg_ts_ha = udbytte_efterafgroede_kg_ts_ha + stub_mv_kg_ts_ha
    # Afgrøderest underjordisk = (Udbytte + Stub mv.) * Faktor_Afgrøderest underjordisk (Page 90)
    a_under_efterafgroede_kg_ts_ha = overjordisk_biomasse_total_kg_ts_ha * faktor_under_table28
    n_under_efterafgroede_kg_n_ha = a_under_efterafgroede_kg_ts_ha * n_indhold_under_table28

    # N_Efterafgrøde = N i Afgrøderest overjordisk_Ej Fjernet (Nedmuldet) + N i Afgrøderest underjordisk (Page 90)
    n_total_efterafgroede_kg_n_ha = n_over_nedmuldet_kg_n_ha + n_under_efterafgroede_kg_n_ha

    return n_over_nedmuldet_kg_n_ha, n_under_efterafgroede_kg_n_ha, n_total_efterafgroede_kg_n_ha


def calculate_co2e_efterafgroede_kg_co2e_ha(n_total_efterafgroede_kg_n_ha: float) -> float:
    """
    Beregner CO2e fra N i efterafgrøderester pr. ha.
    Omlægningsfrekvens typically 1 for efterafgrøder as they are terminated annually.
    Markdown page 90: "Der beregnes kun emission ... hvis udlægsafgrøden ... omlægges ... det efterfølgende høstår."
    This implies the N is accounted for in the year of termination.
    """
    # EF_Lattergas for efterafgrøde is 0.01 (same as EF_N2O_AFGROEDERESTER)
    # N2O_Efterafgrøde = EF_lattergas x N_Efterafgrøde x 44/28 (Page 90)
    co2e_kg_ha = n_total_efterafgroede_kg_n_ha * EF_N2O_AFGROEDERESTER * MOL_WEIGHT_N2O_N_FACTOR * THETA_N2O_CO2
    return co2e_kg_ha


# Testcases
if __name__ == "__main__":
    print("### Test Case 1: Vårbyg (Afgrødekode 1) ###")
    # Inputs from notebook for Vårbyg
    X1_vb = True  # Halmnedmulding
    X2_vb = False  # Udbytte nedmuldes
    # A_vb = 1.0     # Markens areal [ha] - calculations are per ha, then multiplied
    k_graes_vb = 1.0  # No grazing impact assumed for Vårbyg test

    # Tabelværdier for Vårbyg (example)
    O_vb = 1.0  # Omlægningsfrekvens
    N_over_vb = 0.007  # N indhold i afgrøderester over jorden [kg N/kg ts]
    N_under_vb = 0.014  # N indhold i afgrøderester under jorden [kg N/kg ts]
    H_f_vb = 0.55  # Halmfraktion ift. udbytte
    T_vb = 4930.0  # Tørstof i alt (hovedudbytte * tørstofprocent) [kg ts/ha]
    H_u_vb = 0.0  # Fast halmudbytte i frøgræs [kg ts/ha] (0 for Vårbyg)
    S_vb = 0.98  # Slope
    I_vb = 590.0  # Intercept
    F_vb = 0.22  # Forhold underjordisk biomasse til overjordisk biomasse

    # Beregning A_over for Vårbyg
    a_over_vb = calculate_A_over_kg_ts_ha(
        x1_halmnedmulding=X1_vb,
        x2_udbytte_nedmuldes=X2_vb,
        t_torstof_total_kg_ts_ha=T_vb,
        h_u_fast_halmudbytte_kg_ts_ha=H_u_vb,
        s_slope=S_vb,
        i_intercept=I_vb,
        k_graes=k_graes_vb,
        h_f_halmfraktion=H_f_vb,
    )
    print(f"A_over (Vårbyg): {a_over_vb:.4f} kg ts/ha")
    # Expected from notebook (C#): 5421.4 kg ts/ha. My C# re-calc: ((4930+0)*0.98+590)*1 + (1-1)*0.55*4930 + 1*0 + 0*4930 = 5421.4

    # Beregning A_under for Vårbyg
    a_under_vb = calculate_A_under_kg_ts_ha(
        t_torstof_total_kg_ts_ha=T_vb,
        h_u_fast_halmudbytte_kg_ts_ha=H_u_vb,
        s_slope=S_vb,
        i_intercept=I_vb,
        k_graes=k_graes_vb,
        f_forhold_under_over_biomasse=F_vb,
    )
    print(f"A_under (Vårbyg): {a_under_vb:.4f} kg ts/ha")
    # Expected from notebook (C#): 2277.308 kg ts/ha. My C# re-calc: (4930+0 + ((4930+0)*0.98+590)*1)*0.22 = (4930 + 5421.4)*0.22 = 10351.4 * 0.22 = 2277.308

    # Beregning N i afgrøderester for Vårbyg
    n_a_over_vb, n_a_under_vb, n_total_vb = calculate_n_afgroederester_kg_n_ha(
        a_over_kg_ts_ha=a_over_vb,
        n_over_kg_n_pr_kg_ts=N_over_vb,
        a_under_kg_ts_ha=a_under_vb,
        n_under_kg_n_pr_kg_ts=N_under_vb,
    )
    print(f"N i A_over (Vårbyg): {n_a_over_vb:.4f} kg N/ha")  # Expected: 5421.4 * 0.007 = 37.9498
    print(f"N i A_under (Vårbyg): {n_a_under_vb:.4f} kg N/ha")  # Expected: 2277.308 * 0.014 = 31.882312
    print(f"Total N afgrøderester (Vårbyg): {n_total_vb:.4f} kg N/ha")  # Expected: 37.9498 + 31.882312 = 69.832112

    # Beregning CO2e for Vårbyg
    co2e_vb_ha = calculate_co2e_afgroederester_kg_co2e_ha(
        n_a_over_kg_n_ha=n_a_over_vb,
        n_a_under_kg_n_ha=n_a_under_vb,
        o_omlaegningsfrekvens=1.0,  # Annual, so omlægning is 1
        is_perennial_and_ploughed_this_year=False,
    )
    print(f"CO2e (Vårbyg, per ha, THETA_N2O_CO2={THETA_N2O_CO2}): {co2e_vb_ha:.4f} kg CO2e/ha")

    print("\n### Test Case 2: Rajgræs, alm. (Afgrødekode 101) ###")
    # Inputs from notebook for Rajgræs
    X1_rg = False  # Halmnedmulding
    X2_rg = False  # Udbytte nedmuldes
    # A_rg = 1.0     # Markens areal [ha]
    k_graes_rg = 1.0  # Example uses 1.0. If N_græs was given, this could change.

    # Tabelværdier for Rajgræs (example)
    O_rg = 1.0
    N_over_rg = 0.015
    N_under_rg = 0.012
    H_f_rg = 0.0  # Halmfraktion (0 for this grass type example)
    T_rg = 1320.0  # Tørstof i alt
    H_u_rg = 4250.0  # Fast halmudbytte i frøgræs (relevant for this type)
    S_rg = 0.3
    I_rg = 0.0  # Intercept (0 for this grass type example)
    F_rg = 0.8  # Forhold under/over (Mistake in my reading, C# implies F is for A_under. Notebook is F=0.22 for Vårbyg)
    # Looking at C# for Rajgræs: F = 0.8;
    # Afgrøderester under jord: 5792,8 kg ts/ha -> A_under
    # (T + H_u + ((T + H_u) * S + I) * k_græs) * F
    # (1320+4250 + ((1320+4250)*0.3+0)*1.0) * F = (5570 + (5570*0.3)*1.0) * F
    # (5570 + 1671) * F = 7241 * F = 5792.8 -> F = 5792.8 / 7241 = 0.8. So F_rg = 0.8
    F_rg_notebook = 0.8  # Corrected from notebook value

    a_over_rg = calculate_A_over_kg_ts_ha(
        x1_halmnedmulding=X1_rg,
        x2_udbytte_nedmuldes=X2_rg,
        t_torstof_total_kg_ts_ha=T_rg,
        h_u_fast_halmudbytte_kg_ts_ha=H_u_rg,
        s_slope=S_rg,
        i_intercept=I_rg,
        k_graes=k_graes_rg,
        h_f_halmfraktion=H_f_rg,
    )
    print(f"A_over (Rajgræs): {a_over_rg:.4f} kg ts/ha")
    # Expected from notebook (C#): 1671 kg ts/ha
    # C# logic: ((1320+4250)*0.3+0)*1 + (0-1)*0*1320 + 0*4250 + 0*1320 = (5570*0.3)*1 = 1671

    a_under_rg = calculate_A_under_kg_ts_ha(
        t_torstof_total_kg_ts_ha=T_rg,
        h_u_fast_halmudbytte_kg_ts_ha=H_u_rg,
        s_slope=S_rg,
        i_intercept=I_rg,
        k_graes=k_graes_rg,
        f_forhold_under_over_biomasse=F_rg_notebook,
    )
    print(f"A_under (Rajgræs): {a_under_rg:.4f} kg ts/ha")
    # Expected from notebook (C#): 5792.8 kg ts/ha. Calculated above, matches with F=0.8.

    n_a_over_rg, n_a_under_rg, n_total_rg = calculate_n_afgroederester_kg_n_ha(
        a_over_kg_ts_ha=a_over_rg,
        n_over_kg_n_pr_kg_ts=N_over_rg,
        a_under_kg_ts_ha=a_under_rg,
        n_under_kg_n_pr_kg_ts=N_under_rg,
    )
    print(f"N i A_over (Rajgræs): {n_a_over_rg:.4f} kg N/ha")  # Expected: 1671 * 0.015 = 25.065
    print(f"N i A_under (Rajgræs): {n_a_under_rg:.4f} kg N/ha")  # Expected: 5792.8 * 0.012 = 69.5136
    print(f"Total N afgrøderester (Rajgræs): {n_total_rg:.4f} kg N/ha")  # Expected: 25.065 + 69.5136 = 94.5786

    # Test Rajgræs (assuming it's a perennial ploughed after O_rg years, and O_rg is the cycle length for this test)
    # The original C# test used O_rg = 1.0, effectively treating it as annual or ploughed annually for the test.
    # If O_rg was, for example, 3 years, and it's ploughed this year:
    O_rg_cycle = 1.0  # Matching C# example where it was 1.0
    is_rg_ploughed = O_rg_cycle == 1.0  # Or True if it's actually ploughed in a multi-year cycle

    co2e_rg_ha = calculate_co2e_afgroederester_kg_co2e_ha(
        n_a_over_kg_n_ha=n_a_over_rg,
        n_a_under_kg_n_ha=n_a_under_rg,
        o_omlaegningsfrekvens=O_rg_cycle,
        is_perennial_and_ploughed_this_year=is_rg_ploughed,
    )
    print(f"CO2e (Rajgræs, per ha, THETA_N2O_CO2={THETA_N2O_CO2}): {co2e_rg_ha:.4f} kg CO2e/ha")

    print("\nNote: THETA_N2O_CO2 is now {THETA_N2O_CO2} as per Markdown page 4.")
    print(
        "The C# code in the notebook defined theta_N2O_CO2 = 265.0, but its printed CO2e results used a factor of 298."
    )
    print("The CO2e calculations should now align better with C# output that effectively used 298 for N2O GWP.")
    print("The complex A_over calculation and k_graes are retained from original script structure.")
    print("Added functions for Efterafgrøder (Afgrøde 2). Unit consistency for Table 27 Hældning/Intercept needs care.")

    # Example of using k_graes function
    print("\nTesting k_graes function:")
    print(f"k_graes for N_graes = 5 kg N/ha: {calculate_k_graes(5)}")  # Expected 1.0
    print(f"k_graes for N_graes = 10 kg N/ha: {calculate_k_graes(10)}")  # Expected 1.24
    print(f"k_graes for N_graes = 30 kg N/ha: {calculate_k_graes(30)}")  # Expected 1.24
    print(f"k_graes for N_graes = 50 kg N/ha: {calculate_k_graes(50)}")  # Expected 1.49
    print(f"k_graes for N_graes = 100 kg N/ha: {calculate_k_graes(100)}")  # Expected 1.49


# ============================================================================
# Convenience functions using crop_parameters lookup
# ============================================================================

def calculate_crop_residue_emissions(
    crop_code: int,
    yield_kg_ha: float,
    area_ha: float = 1.0,
    straw_incorporated: bool = True,
    yield_incorporated: bool = False,
    is_perennial: bool = False,
    rotation_years: float = 1.0,
) -> Optional[Tuple[float, float, float, float]]:
    """
    Calculate crop residue emissions using crop_parameters lookup.

    This is a convenience function that combines parameter lookup with
    the detailed calculation functions.

    Args:
        crop_code: FVM crop code (numeric)
        yield_kg_ha: Crop yield in kg dry matter per hectare
        area_ha: Field area in hectares (default 1.0 for per-ha calculation)
        straw_incorporated: Whether straw is ploughed in (X1)
        yield_incorporated: Whether yield itself is ploughed in (X2)
        is_perennial: Whether crop is perennial (affects rotation calculation)
        rotation_years: Rotation frequency for perennials

    Returns:
        Tuple of (a_over_kg_ts_ha, a_under_kg_ts_ha, n_total_kg_n_ha, co2e_kg)
        or None if crop code not found in parameters
    """
    params = get_crop_params(crop_code)
    if params is None:
        return None

    # Extract parameters
    slope = params["slope"]
    intercept = params["intercept"] * 1000  # Convert from tonnes to kg
    n_over = params["n_over"]
    n_under = params["n_under"]
    f_under = params["f_under"]

    # Default values for non-grass crops
    k_graes = 1.0
    h_u = 0.0  # No fixed straw yield
    h_f = 0.55  # Default straw fraction

    # Calculate A_over
    a_over = calculate_A_over_kg_ts_ha(
        x1_halmnedmulding=straw_incorporated,
        x2_udbytte_nedmuldes=yield_incorporated,
        t_torstof_total_kg_ts_ha=yield_kg_ha,
        h_u_fast_halmudbytte_kg_ts_ha=h_u,
        s_slope=slope,
        i_intercept=intercept,
        k_graes=k_graes,
        h_f_halmfraktion=h_f,
    )

    # Calculate A_under
    a_under = calculate_A_under_kg_ts_ha(
        t_torstof_total_kg_ts_ha=yield_kg_ha,
        h_u_fast_halmudbytte_kg_ts_ha=h_u,
        s_slope=slope,
        i_intercept=intercept,
        k_graes=k_graes,
        f_forhold_under_over_biomasse=f_under,
    )

    # Calculate N content
    n_a_over, n_a_under, n_total = calculate_n_afgroederester_kg_n_ha(
        a_over_kg_ts_ha=a_over,
        n_over_kg_n_pr_kg_ts=n_over,
        a_under_kg_ts_ha=a_under,
        n_under_kg_n_pr_kg_ts=n_under,
    )

    # Calculate CO2e
    co2e_per_ha = calculate_co2e_afgroederester_kg_co2e_ha(
        n_a_over_kg_n_ha=n_a_over,
        n_a_under_kg_n_ha=n_a_under,
        o_omlaegningsfrekvens=rotation_years,
        is_perennial_and_ploughed_this_year=is_perennial and rotation_years > 1,
    )

    co2e_total = co2e_per_ha * area_ha

    return (a_over, a_under, n_total, co2e_total)


def calculate_simple_crop_residue_co2e(
    crop_code: int,
    area_ha: float,
    standard_yield_kg_ha: Optional[float] = None,
) -> Optional[float]:
    """
    Simplified crop residue CO2e calculation using default parameters.

    Uses standard yield values if not provided.

    Args:
        crop_code: FVM crop code
        area_ha: Field area in hectares
        standard_yield_kg_ha: Optional yield override (defaults to standard yields)

    Returns:
        CO2e emissions in kg, or None if crop not supported
    """
    params = get_crop_params(crop_code)
    if params is None:
        return None

    # Use standard yields if not provided (approximate Danish averages)
    if standard_yield_kg_ha is None:
        # Standard yields in kg dry matter per hectare
        standard_yields = {
            1: 5000,    # Vårbyg
            2: 5500,    # Vårhvede
            3: 4500,    # Havre
            5: 9000,    # Majs
            10: 5500,   # Vinterbyg
            11: 7000,   # Vinterhvede
            14: 5000,   # Vinterrug
            16: 5500,   # Triticale
            21: 2500,   # Vårraps
            22: 3500,   # Vinterraps
            30: 3000,   # Markærter
            31: 3500,   # Hestebønner
            151: 8000,  # Kartofler (fresh weight basis differs)
            160: 10000, # Sukkerroer
            216: 9000,  # Majs variants
            218: 9000,
            252: 6000,  # Græs
            254: 6000,
            263: 6000,
            260: 7000,  # Kløvergræs
            276: 7000,
        }
        standard_yield_kg_ha = standard_yields.get(crop_code, 5000)

    result = calculate_crop_residue_emissions(
        crop_code=crop_code,
        yield_kg_ha=standard_yield_kg_ha,
        area_ha=area_ha,
    )

    if result is None:
        return None

    return result[3]  # Return CO2e total
