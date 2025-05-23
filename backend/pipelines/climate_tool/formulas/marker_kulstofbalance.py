"""
Beregning af kulstofbalance på marken.

OBS: Denne fil indeholder placeholder-funktioner for beregning af A_over og A_under,
     som skal erstattes med faktiske kald til funktioner fra marker_afgroederester.py
     når denne er implementeret.
"""

# Constants from the notebook
C_FRAK_TORSTOF_TIL_C = 0.45  # Den antagede kulstoffraktion i tørstof [kg C/kg ts]
MU_DK_GENNEMSNIT_INPUT_C = 4093.0  # Det gennemsnitlige danske input af kulstof fra en afgrøde [kg C/ha]
F_HUS_N_TIL_C_ORGANISK = 8.0  # Omregningsfaktor fra N til C for husdyrgødning [kg C/kg N]
MOL_WEIGHT_CO2_C_FACTOR = 44.0 / 12.0 # Omregningsfaktor C til CO2
STABILIZATION_FACTOR = 0.097 # Faktor for stabilisering af C i jord

# Placeholder functions - these should eventually call functions from marker_afgroederester.py
def calculate_A_over_placeholder(afgroede_kode: int, **kwargs) -> float:
    """Placeholder for A_over from Afgrøderester.ipynb"""
    # Test values from Kulstofbalance.ipynb notebook
    if afgroede_kode == 1: # Vårbyg example
        return 4755.0 # kg ts/ha
    elif afgroede_kode == 945: # Kløvergræs example
        return 390.59999999999997 # kg ts/ha
    print(f"Warning: Using placeholder A_over for afgroede_kode {afgroede_kode}. Replace with actual calculation.")
    return 0.0 # Default, should be based on actual crop data

def calculate_A_under_placeholder(afgroede_kode: int, **kwargs) -> float:
    """Placeholder for A_under from Afgrøderester.ipynb"""
    # Test values from Kulstofbalance.ipynb notebook
    if afgroede_kode == 1: # Vårbyg example
        return 1981.1 # kg ts/ha
    elif afgroede_kode == 945: # Kløvergræs example
        return 1354.08 # kg ts/ha
    print(f"Warning: Using placeholder A_under for afgroede_kode {afgroede_kode}. Replace with actual calculation.")
    return 0.0 # Default, should be based on actual crop data

def calculate_C_afgroederest_kg_c_ha(
    a_over_kg_ts_ha: float,
    a_under_kg_ts_ha: float
) -> float:
    """
    Beregner kulstof (C) fra afgrøderester i kg C pr. ha.
    Formel: C_afgrøderest = (A_over + A_under) * C_frak
    """
    return (a_over_kg_ts_ha + a_under_kg_ts_ha) * C_FRAK_TORSTOF_TIL_C

def calculate_C_organisk_goedning_kg_c_ha(n_hus_plus_afg_kg_n_ha: float) -> float:
    """
    Beregner kulstof (C) fra organisk gødning (husdyrgødning og afgræsning) i kg C pr. ha.
    Formel: C_organisk = N_hus+afg * f_hus
    """
    return n_hus_plus_afg_kg_n_ha * F_HUS_N_TIL_C_ORGANISK

def calculate_co2e_kulstofbalance_mark(
    r_relativ_faktor: int, # 0, 1, or 2
    areal_ha: float,
    c_afgroederest_kg_c_ha: float,
    c_organisk_kg_c_ha: float
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
        return 0.0 # CO2e er 0
    else:
        raise ValueError(f"Ugyldig R faktor: {r_relativ_faktor}. Skal være 0, 1, eller 2.")

    # Formel: C_balance_kg_c_ha_total * Areal * (44/12) * 0,097 * -1
    co2e_kulstofbalance = (
        c_balance_kg_c_ha * areal_ha * MOL_WEIGHT_CO2_C_FACTOR * STABILIZATION_FACTOR * -1.0
    )
    return co2e_kulstofbalance


# Testcases based on Marker/Kulstofbalance.ipynb
if __name__ == "__main__":
    # Test Case 1: Vårbyg (Afgrødekode 1, antag R=1 for Vårbyg as per example logic)
    print("Test Case 1: Vårbyg (R=1)")
    AFGROEDEKODE_VAARBYG = 1
    R_VAARBYG = 1
    A_OVER_VAARBYG_TS_HA = calculate_A_over_placeholder(AFGROEDEKODE_VAARBYG)
    A_UNDER_VAARBYG_TS_HA = calculate_A_under_placeholder(AFGROEDEKODE_VAARBYG)
    N_HUS_VAARBYG_KG_N_HA = 7.0
    AREAL_VAARBYG_HA = 1.0

    c_afgroederest_vb = calculate_C_afgroederest_kg_c_ha(A_OVER_VAARBYG_TS_HA, A_UNDER_VAARBYG_TS_HA)
    print(f"  C i afgrøderester (Vårbyg): {c_afgroederest_vb:.4f} kg C/ha")
    # Expected: (4755.0 + 1981.1) * 0.45 = 6736.1 * 0.45 = 3031.245

    c_organisk_vb = calculate_C_organisk_goedning_kg_c_ha(N_HUS_VAARBYG_KG_N_HA)
    print(f"  C i organisk materiale (Vårbyg): {c_organisk_vb:.4f} kg C/ha")
    # Expected: 7.0 * 8.0 = 56.0

    co2e_vb = calculate_co2e_kulstofbalance_mark(
        R_VAARBYG, AREAL_VAARBYG_HA, c_afgroederest_vb, c_organisk_vb
    )
    # Notebook calculation for C_kulstofbalance: (3031.245 + 56.0 - 4093.0) * 1.0 * -1 = -1005.755 * -1 = 1005.755 kg C
    # Notebook CO2e: 1005.755 * (44/12) * 0.097 = 357.713528... kg CO2e
    print(f"  Kulstofbalance CO2e (Vårbyg): {co2e_vb:.4f} kg CO2e")
    # My code should yield the same as notebook if placeholders match.

    print("\nTest Case 2: Kløvergræs (Afgrødekode 945, antag R=0 as per example logic)")
    AFGROEDEKODE_KLOVERGRAES = 945
    R_KLOVERGRAES = 0
    A_OVER_KLOVERGRAES_TS_HA = calculate_A_over_placeholder(AFGROEDEKODE_KLOVERGRAES)
    A_UNDER_KLOVERGRAES_TS_HA = calculate_A_under_placeholder(AFGROEDEKODE_KLOVERGRAES)
    N_HUS_KLOVERGRAES_KG_N_HA = 7.0
    AREAL_KLOVERGRAES_HA = 1.0

    c_afgroederest_kg = calculate_C_afgroederest_kg_c_ha(A_OVER_KLOVERGRAES_TS_HA, A_UNDER_KLOVERGRAES_TS_HA)
    print(f"  C i afgrøderester (Kløvergræs): {c_afgroederest_kg:.4f} kg C/ha")
    # Expected: (390.59999999999997 + 1354.08) * 0.45 = 1744.67999... * 0.45 = 785.106

    c_organisk_kg = calculate_C_organisk_goedning_kg_c_ha(N_HUS_KLOVERGRAES_KG_N_HA)
    print(f"  C i organisk materiale (Kløvergræs): {c_organisk_kg:.4f} kg C/ha")
    # Expected: 7.0 * 8.0 = 56.0

    co2e_kg = calculate_co2e_kulstofbalance_mark(
        R_KLOVERGRAES, AREAL_KLOVERGRAES_HA, c_afgroederest_kg, c_organisk_kg
    )
    # Notebook calculation for C_kulstofbalance: (785.106 + 56.0) * 1.0 * -1 = -841.106 kg C
    # Notebook CO2e: -841.106 * (44/12) * 0.097 = -299.1533... kg CO2e
    print(f"  Kulstofbalance CO2e (Kløvergræs): {co2e_kg:.4f} kg CO2e")

    print("\nTest Case 3: JB11 Jordtype (R=2)")
    R_JB11 = 2
    # Other inputs don't matter if R=2
    co2e_jb11 = calculate_co2e_kulstofbalance_mark(R_JB11, 1.0, 1000, 100)
    print(f"  Kulstofbalance CO2e (JB11, R=2): {co2e_jb11:.4f} kg CO2e")
    # Expected: 0.0

    print("\nNOTE: A_over and A_under are using placeholder values from the Kulstofbalance notebook examples.")
    print("These need to be replaced with calls to actual calculations from marker_afgroederester.py once available.")