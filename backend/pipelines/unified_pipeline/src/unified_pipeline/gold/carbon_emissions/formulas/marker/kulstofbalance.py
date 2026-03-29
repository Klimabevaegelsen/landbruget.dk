"""
Beregning af kulstofbalance på marken.

Uses actual A_over and A_under calculations from afgroederester.py
with crop parameters from crop_parameters.py.
"""

# Import crop residue calculations from afgroederester
from .afgroederester import calculate_crop_residue_emissions

# Constants from the notebook
C_FRAK_TORSTOF_TIL_C = 0.45  # Den antagede kulstoffraktion i tørstof [kg C/kg ts]
MU_DK_GENNEMSNIT_INPUT_C = (
    4093.0  # Det gennemsnitlige danske input af kulstof fra en afgrøde [kg C/ha]
)
F_HUS_N_TIL_C_ORGANISK = 8.0  # Omregningsfaktor fra N til C for husdyrgødning [kg C/kg N]
MOL_WEIGHT_CO2_C_FACTOR = 44.0 / 12.0  # Omregningsfaktor C til CO2
STABILIZATION_FACTOR = 0.097  # Faktor for stabilisering af C i jord


def get_A_over_A_under_for_crop(  # noqa: N802
    crop_code: int,
    yield_kg_ts_ha: float,
    straw_incorporated: bool = True,
    yield_incorporated: bool = False,
) -> tuple[float, float] | None:
    """
    Get A_over and A_under for a crop using actual formulas.

    Args:
        crop_code: FVM crop code
        yield_kg_ts_ha: Harvested yield in kg DRY MATTER per hectare
        straw_incorporated: Whether straw is ploughed in (default True)
        yield_incorporated: Whether yield itself is ploughed in (default False)

    Returns:
        Tuple of (A_over_kg_ts_ha, A_under_kg_ts_ha) or None if crop not found
    """
    result = calculate_crop_residue_emissions(
        crop_code=crop_code,
        yield_kg_ha=yield_kg_ts_ha,
        area_ha=1.0,
        straw_incorporated=straw_incorporated,
        yield_incorporated=yield_incorporated,
    )

    if result is None:
        return None

    a_over, a_under, _, _ = result
    return (a_over, a_under)


def calculate_C_afgroederest_kg_c_ha(a_over_kg_ts_ha: float, a_under_kg_ts_ha: float) -> float:  # noqa: N802
    """
    Beregner kulstof (C) fra afgrøderester i kg C pr. ha.
    Formel: C_afgrøderest = (A_over + A_under) * C_frak
    """
    return (a_over_kg_ts_ha + a_under_kg_ts_ha) * C_FRAK_TORSTOF_TIL_C


def calculate_C_organisk_goedning_kg_c_ha(n_hus_plus_afg_kg_n_ha: float) -> float:  # noqa: N802
    """
    Beregner kulstof (C) fra organisk gødning (husdyrgødning og afgræsning) i kg C pr. ha.
    Formel: C_organisk = N_hus+afg * f_hus
    """
    return n_hus_plus_afg_kg_n_ha * F_HUS_N_TIL_C_ORGANISK


def calculate_co2e_kulstofbalance_mark(
    r_relativ_faktor: int,  # 0, 1, or 2
    areal_ha: float,
    c_afgroederest_kg_c_ha: float,
    c_organisk_kg_c_ha: float,
) -> float:
    """
    Beregner CO2e fra kulstofbalancen for en mark.

    Args:
        r_relativ_faktor (int): Indikerer om afgrøden skal relativeres til DK gennemsnit (μ_DK).
                                0 = ikke relativ, 1 = relativ, 2 = nulstilles (f.eks. JB11).
        areal_ha (float): Markens samlede areal [ha].
        c_afgroederest_kg_c_ha (float): Kulstof fra afgrøderester [kg C/ha].
        c_organisk_kg_c_ha (float): Kulstof fra organisk gødning [kg C/ha].

    Returns:
        float: CO2e_kulstofbalance i kg CO2e (negativt fortegn indikerer binding).
    """
    c_balance_kg_c_ha = 0.0

    if r_relativ_faktor == 1:
        # (C_afgrøderest + C_organisk - μ_DK)
        c_balance_kg_c_ha = c_afgroederest_kg_c_ha + c_organisk_kg_c_ha - MU_DK_GENNEMSNIT_INPUT_C
    elif r_relativ_faktor == 0:
        # (C_afgrøderest + C_organisk)
        c_balance_kg_c_ha = c_afgroederest_kg_c_ha + c_organisk_kg_c_ha
    elif r_relativ_faktor == 2:
        return 0.0  # CO2e er 0
    else:
        raise ValueError(f"Ugyldig R faktor: {r_relativ_faktor}. Skal være 0, 1, eller 2.")

    # Formel: C_balance_kg_c_ha_total * Areal * (44/12) * 0,097 * -1
    return c_balance_kg_c_ha * areal_ha * MOL_WEIGHT_CO2_C_FACTOR * STABILIZATION_FACTOR * -1.0


# Testcases based on Marker/Kulstofbalance.ipynb
if __name__ == "__main__":
    # Test Case 1: Vårbyg (crop_code 1, R=1)
    # Using typical yield of 5500 kg/ha
    print("Test Case 1: Vårbyg (R=1)")
    CROP_CODE_VAARBYG = 1
    YIELD_VAARBYG_KG_HA = 5500  # Typical spring barley yield
    R_VAARBYG = 1
    N_HUS_VAARBYG_KG_N_HA = 7.0
    AREAL_VAARBYG_HA = 1.0

    result_vb = get_A_over_A_under_for_crop(CROP_CODE_VAARBYG, YIELD_VAARBYG_KG_HA)
    if result_vb:
        A_OVER_VAARBYG_TS_HA, A_UNDER_VAARBYG_TS_HA = result_vb
        print(f"  A_over (Vårbyg): {A_OVER_VAARBYG_TS_HA:.2f} kg ts/ha")
        print(f"  A_under (Vårbyg): {A_UNDER_VAARBYG_TS_HA:.2f} kg ts/ha")

        c_afgroederest_vb = calculate_C_afgroederest_kg_c_ha(
            A_OVER_VAARBYG_TS_HA, A_UNDER_VAARBYG_TS_HA
        )
        print(f"  C i afgrøderester (Vårbyg): {c_afgroederest_vb:.4f} kg C/ha")

        c_organisk_vb = calculate_C_organisk_goedning_kg_c_ha(N_HUS_VAARBYG_KG_N_HA)
        print(f"  C i organisk materiale (Vårbyg): {c_organisk_vb:.4f} kg C/ha")

        co2e_vb = calculate_co2e_kulstofbalance_mark(
            R_VAARBYG, AREAL_VAARBYG_HA, c_afgroederest_vb, c_organisk_vb
        )
        print(f"  Kulstofbalance CO2e (Vårbyg): {co2e_vb:.4f} kg CO2e")
    else:
        print("  ERROR: Vårbyg (crop_code 1) not found in crop_parameters!")

    # Test Case 2: Kløvergræs (crop_code 260, R=0)
    # Using typical yield of 8000 kg/ha
    print("\nTest Case 2: Kløvergræs (R=0)")
    CROP_CODE_KLOVERGRAES = 260
    YIELD_KLOVERGRAES_KG_HA = 8000  # Typical clover-grass yield
    R_KLOVERGRAES = 0
    N_HUS_KLOVERGRAES_KG_N_HA = 7.0
    AREAL_KLOVERGRAES_HA = 1.0

    result_kg = get_A_over_A_under_for_crop(CROP_CODE_KLOVERGRAES, YIELD_KLOVERGRAES_KG_HA)
    if result_kg:
        A_OVER_KLOVERGRAES_TS_HA, A_UNDER_KLOVERGRAES_TS_HA = result_kg
        print(f"  A_over (Kløvergræs): {A_OVER_KLOVERGRAES_TS_HA:.2f} kg ts/ha")
        print(f"  A_under (Kløvergræs): {A_UNDER_KLOVERGRAES_TS_HA:.2f} kg ts/ha")

        c_afgroederest_kg = calculate_C_afgroederest_kg_c_ha(
            A_OVER_KLOVERGRAES_TS_HA, A_UNDER_KLOVERGRAES_TS_HA
        )
        print(f"  C i afgrøderester (Kløvergræs): {c_afgroederest_kg:.4f} kg C/ha")

        c_organisk_kg = calculate_C_organisk_goedning_kg_c_ha(N_HUS_KLOVERGRAES_KG_N_HA)
        print(f"  C i organisk materiale (Kløvergræs): {c_organisk_kg:.4f} kg C/ha")

        co2e_kg = calculate_co2e_kulstofbalance_mark(
            R_KLOVERGRAES, AREAL_KLOVERGRAES_HA, c_afgroederest_kg, c_organisk_kg
        )
        print(f"  Kulstofbalance CO2e (Kløvergræs): {co2e_kg:.4f} kg CO2e")
    else:
        print("  ERROR: Kløvergræs (crop_code 260) not found in crop_parameters!")

    print("\nTest Case 3: JB11 Jordtype (R=2)")
    R_JB11 = 2
    # Other inputs don't matter if R=2
    co2e_jb11 = calculate_co2e_kulstofbalance_mark(R_JB11, 1.0, 1000, 100)
    print(f"  Kulstofbalance CO2e (JB11, R=2): {co2e_jb11:.4f} kg CO2e")
    # Expected: 0.0

    # Test Case 4: Crop not in mapping (should return None)
    print("\nTest Case 4: Unknown crop (crop_code 9999)")
    result_unknown = get_A_over_A_under_for_crop(9999, 5000)
    if result_unknown is None:
        print("  Correctly returned None for unknown crop")
    else:
        print("  ERROR: Should have returned None!")
