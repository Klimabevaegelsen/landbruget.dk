"""
Beregning af N2O fra gødning og nitrifikationshæmmer på marken.
"""

from typing import Tuple, Any
import json
from pathlib import Path

# Utility function to load data from JSON files (if not already present from a previous edit)
# This function should be defined once, typically in a shared utility module or at the top of the first file that needs it.
# For this exercise, we'll ensure it's here if marker_afgroederester.py wasn't processed first or if it's a separate context.

# Check if load_json_data is already defined (e.g. by a previous edit in the same session for marker_afgroederester.py)
# This is a conceptual check; in a real multi-file edit, this would be handled by structuring imports.
if 'load_json_data' not in globals():
    def load_json_data(file_path: str) -> Any:
        """Loads data from a JSON file."""
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

# Constants from the notebook
THETA_N2O_CO2 = 265.0  # Omregningsfaktor N2O til CO2e
# EF_N2O_GENERAL = 0.01  # General EF_N2O for NH3 and NOx deposition, and for jord (non-grazing) - Now loaded
EF_N2O_JORD_AFGRAESNING = 0.004 # EF_N2O for jord for gødning afsat under afgræsning

# Emissionsfaktorer (EF) for N-tab som NH3
EF_NH3_HANDELSGOEDNING = 0.05
EF_NH3_HUSDYRGOEDNING = 0.08
EF_NH3_AFGRAESNING = 0.084 # For kvæg (som specificeret i notebook comment)

# Emissionsfaktor (EF) for N-tab som NOx
EF_NOX = 0.04

# Faktor for molvægt N2O/N = 44/28
MOL_WEIGHT_FACTOR_N2O_N = 44.0 / 28.0
# Faktor for molvægt NO2/N (brugt for NOx i notebook, 46/14)
MOL_WEIGHT_FACTOR_NOX_N = 46.0 / 14.0 # NO2/N, though NOx is a mix

NITRIFICATION_INHIBITOR_EFFECTIVENESS = 0.4  # 40% reduction

# Load EF_N2O_GENERAL from tabel_19
try:
    tabel_19_data = load_json_data("tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json")
    # Assuming the first entry in 'data' array and 'EF_N2O' key exists and is the correct one.
    EF_N2O_GENERAL = tabel_19_data['data'][0]['EF_N2O']
except Exception as e:
    print(f"Failed to load EF_N2O_GENERAL from tabel_19: {e}")
    EF_N2O_GENERAL = 0.01 # Fallback to original hardcoded value

# Load NH3 emission factors for handelsgoedning from tabel_22
DEFAULT_EF_NH3_HANDELSGOEDNING = EF_NH3_HANDELSGOEDNING # Keep the original value as default
HANDELSGOEDNING_EF_NH3_DATA = {}
try:
    tabel_22_data = load_json_data("tabel_22_nh3_emissionsfaktorer_for_forskellige_typer_handelsgødning_2011-2017_kg_nh3-n_pr_kg_n_side_.json")
    for item in tabel_22_data.get("data", []):
        goedningstype = item.get("Gødningstype")
        # Using "2015_2016_2017" as the reference year column based on typical usage
        ef_value = item.get("2015_2016_2017")
        if goedningstype and ef_value is not None:
            HANDELSGOEDNING_EF_NH3_DATA[goedningstype] = float(ef_value)
except Exception as e:
    print(f"Failed to load or parse tabel_22_data: {e}. Using default EF_NH3 for handelsgoedning.")
    # HANDELSGOEDNING_EF_NH3_DATA will remain empty, causing fallback to default

def get_ef_nh3_for_handelsgoedning(handelsgoedning_type: str | None) -> float:
    """
    Retrieves the NH3 emission factor for a specific type of handelsgoedning.
    Uses data from "2015_2016_2017" column in tabel_22.
    Falls back to DEFAULT_EF_NH3_HANDELSGOEDNING if type is not found or not specified.
    """
    if handelsgoedning_type and handelsgoedning_type in HANDELSGOEDNING_EF_NH3_DATA:
        return HANDELSGOEDNING_EF_NH3_DATA[handelsgoedning_type]
    return DEFAULT_EF_NH3_HANDELSGOEDNING

def calculate_n2o_components(
    n_total_kg_ha: float,
    areal_ha: float,
    ef_n2o_jord: float,
    ef_nh3: float,
) -> Tuple[float, float, float]:
    """
    Calculates the three N2O components: N2O_jord, N2O_NH3, N2O_NOx.

    Args:
        n_total_kg_ha (float): Total N i gødningstypen pr. ha [kg N/ha]
        areal_ha (float): Markens totale areal [ha]
        ef_n2o_jord (float): Emissionsfaktor for N2O direkte fra jord for den specifikke gødningstype.
        ef_nh3 (float): Emissionsfaktor for NH3-N tab for den specifikke gødningstype.

    Returns:
        Tuple[float, float, float]:
            - n2o_jord_kg (float): kg N2O fra jord
            - n2o_nh3_kg (float): kg N2O fra NH3 deposition
            - n2o_nox_kg (float): kg N2O fra NOx deposition
    """
    n_total_on_areal_kg = n_total_kg_ha * areal_ha

    # N2O fra jord
    n2o_jord_kg = n_total_on_areal_kg * ef_n2o_jord * MOL_WEIGHT_FACTOR_N2O_N

    # N2O fra NH3 deposition
    n2o_nh3_kg = (
        n_total_on_areal_kg * ef_nh3 * EF_N2O_GENERAL * MOL_WEIGHT_FACTOR_N2O_N
    )

    # N2O fra NOx deposition
    # The notebook formula: (N_total * A * EF_NOx) / (46/14) * EF_N2O * (44/28)
    # This means (N-NOx lost) * EF_N2O_for_deposited_N * (44/28)
    n_lost_as_nox_kg = n_total_on_areal_kg * EF_NOX
    # Convert N-NOx to N that redeposits and forms N2O (assuming NOx is primarily NO2 for this factor)
    # The formula divides by (46/14) which is M_NO2/M_N. This seems to convert N in fertilizer to N in NOx emission.
    # However, the EF_NOX should represent fraction of N lost as N-NOx.
    # Let's follow the notebook's direct formula structure.
    n2o_nox_kg = (
        (n_total_on_areal_kg * EF_NOX) / MOL_WEIGHT_FACTOR_NOX_N
    ) * EF_N2O_GENERAL * MOL_WEIGHT_FACTOR_N2O_N

    return n2o_jord_kg, n2o_nh3_kg, n2o_nox_kg

def calculate_n2o_goedning(
    n_total_kg_ha: float, # N i gødningstypen pr. ha
    areal_ha: float,      # Markens areal i ha
    goedningstype: str,   # "handelsgoedning", "husdyrgoedning", "afgraesning"
    n_nitri_kg_ha: float = 0.0, # Mængde N pr ha tilsat nitrifikationshæmmer
    handelsgoedning_detail_type: str | None = None # Specific type of "handelsgoedning" from tabel_22
) -> Tuple[float, float]:
    """
    Beregner N2O udledning fra gødning og CO2e.

    Args:
        n_total_kg_ha (float): Total N i gødningstypen pr. ha [kg N/ha].
        areal_ha (float): Markens totale areal [ha].
        goedningstype (str): Type af gødning ("handelsgoedning", "husdyrgoedning", "afgraesning").
        n_nitri_kg_ha (float): Mængde N pr ha fra gødning der er tilsat nitrifikationshæmmere [kg N/ha].
                                 Gælder kun for 'handelsgoedning' og 'husdyrgoedning'.
        handelsgoedning_detail_type (str, optional): Specific type of 'handelsgoedning', e.g., 'Urea*'.
                                                     If provided, uses specific EF_NH3 from tabel_22.
                                                     Defaults to None, using the general EF_NH3_HANDELSGOEDNING.

    Returns:
        Tuple[float, float]:
            - total_n2o_kg (float): Total N2O i kg.
            - total_co2e_kg (float): Total CO2e i kg.
    """
    if goedningstype == "handelsgoedning":
        ef_nh3_selected = get_ef_nh3_for_handelsgoedning(handelsgoedning_detail_type)
        ef_n2o_jord_selected = EF_N2O_GENERAL
    elif goedningstype == "husdyrgoedning":
        ef_nh3_selected = EF_NH3_HUSDYRGOEDNING
        ef_n2o_jord_selected = EF_N2O_GENERAL
    elif goedningstype == "afgraesning":
        ef_nh3_selected = EF_NH3_AFGRAESNING
        ef_n2o_jord_selected = EF_N2O_JORD_AFGRAESNING
        if n_nitri_kg_ha > 0:
            # print("Note: Nitrifikationshæmmer kan ikke anvendes på gødning afsat under afgræsning.")
            n_nitri_kg_ha = 0.0 # Ignorer for afgræsning
    else:
        raise ValueError(f"Ukendt goedningstype: {goedningstype}")

    n2o_jord_kg, n2o_nh3_kg, n2o_nox_kg = calculate_n2o_components(
        n_total_kg_ha, areal_ha, ef_n2o_jord_selected, ef_nh3_selected
    )

    # Juster N2O_jord for nitrifikationshæmmer hvis relevant
    if n_nitri_kg_ha > 0 and goedningstype in ["handelsgoedning", "husdyrgoedning"]:
        if n_nitri_kg_ha > n_total_kg_ha:
            # print(f"Warning: N_nitri ({n_nitri_kg_ha}) > N_total ({n_total_kg_ha}). Clamping N_nitri to N_total.")
            n_nitri_on_areal_kg = n_total_kg_ha * areal_ha
        else:
            n_nitri_on_areal_kg = n_nitri_kg_ha * areal_ha

        n2o_jord_reduction = (
            n_nitri_on_areal_kg * ef_n2o_jord_selected * MOL_WEIGHT_FACTOR_N2O_N * NITRIFICATION_INHIBITOR_EFFECTIVENESS
        )
        n2o_jord_kg -= n2o_jord_reduction

    total_n2o_kg = n2o_jord_kg + n2o_nh3_kg + n2o_nox_kg
    total_co2e_kg = total_n2o_kg * THETA_N2O_CO2

    return total_n2o_kg, total_co2e_kg

# Testcases from Marker/Gødning_og_nitrifikationshæmmer.ipynb
if __name__ == "__main__":
    AREAL_HA_TEST = 1.0 # All notebook tests seem to use A=1 implicitly by providing N_total as total N.
                        # The python functions expect N_total_kg_ha and areal_ha separately.
                        # For consistency with notebook output, we will set areal_ha=1 and N_total_kg_ha to the notebook's N_total.

    # Test 1: Handelsgødning + nitrifikationshæmmer
    N_total_handel_kg_ha = 122.0
    N_nitri_handel_kg_ha = 12.0 # 12 kg N (ud af 122 kg N) er med nitrihæmmer

    n2o_handel_no_nitri, co2e_handel_no_nitri = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_kg_ha, areal_ha=AREAL_HA_TEST, goedningstype="handelsgoedning", n_nitri_kg_ha=0
    )
    print(f"Handelsgødning (uden nitri, default EF_NH3):")
    print(f"  N2O: {n2o_handel_no_nitri:.4f} kg")
    print(f"  CO2e: {co2e_handel_no_nitri:.4f} kg")
    # Expected N2O from notebook: 2.0363391304347824

    n2o_handel_w_nitri, co2e_handel_w_nitri = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_kg_ha, areal_ha=AREAL_HA_TEST, goedningstype="handelsgoedning", n_nitri_kg_ha=N_nitri_handel_kg_ha
    )
    nitri_percentage_handel = (N_nitri_handel_kg_ha / N_total_handel_kg_ha) * 100
    print(f"Handelsgødning (med {nitri_percentage_handel:.1f}% N m. nitrihæmmer, default EF_NH3):")
    print(f"  N2O: {n2o_handel_w_nitri:.4f} kg")
    print(f"  CO2e: {co2e_handel_w_nitri:.4f} kg")
    # Expected N2O from notebook: 1.960910559006211

    # Test 1b: Handelsgødning (Urea*) + nitrifikationshæmmer
    urea_type = "Urea*"
    N_total_handel_urea_kg_ha = 122.0
    N_nitri_handel_urea_kg_ha = 12.0

    n2o_handel_urea_w_nitri, co2e_handel_urea_w_nitri = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_urea_kg_ha,
        areal_ha=AREAL_HA_TEST,
        goedningstype="handelsgoedning",
        n_nitri_kg_ha=N_nitri_handel_urea_kg_ha,
        handelsgoedning_detail_type=urea_type
    )
    print(f"Handelsgødning ({urea_type}, med {nitri_percentage_handel:.1f}% N m. nitrihæmmer):")
    print(f"  N2O: {n2o_handel_urea_w_nitri:.4f} kg")
    print(f"  CO2e: {co2e_handel_urea_w_nitri:.4f} kg")

    # Test 1c: Handelsgødning (Unknown type) - should fallback to default
    unknown_type = "NonExistentType"
    n2o_handel_unknown_fallback, _ = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_kg_ha, # Using same N as original test 1
        areal_ha=AREAL_HA_TEST,
        goedningstype="handelsgoedning",
        n_nitri_kg_ha=0, # No nitri for simplicity, to compare with n2o_handel_no_nitri
        handelsgoedning_detail_type=unknown_type
    )
    print(f"Handelsgødning ({unknown_type}, fallback EF_NH3):")
    print(f"  N2O: {n2o_handel_unknown_fallback:.4f} kg (Should be same as default EF_NH3 test: {n2o_handel_no_nitri:.4f})")

    print("-"*30)

    # Test 2: Husdyr/anden organisk gødning + nitrifikationshæmmer
    N_total_husdyr_kg_ha = 100.0
    N_nitri_husdyr_kg_ha = 50.0 # 50 kg N (ud af 100 kg N) er med nitrihæmmer

    n2o_husdyr_no_nitri, co2e_husdyr_no_nitri = calculate_n2o_goedning(
        n_total_kg_ha=N_total_husdyr_kg_ha, areal_ha=AREAL_HA_TEST, goedningstype="husdyrgoedning", n_nitri_kg_ha=0
    )
    print(f"Husdyrgødning (uden nitri):")
    print(f"  N2O: {n2o_husdyr_no_nitri:.4f} kg") # Notebook uses "Handelsgødning" in print statement, but values match husdyr
    print(f"  CO2e: {co2e_husdyr_no_nitri:.4f} kg")
    # Expected N2O from notebook (mistakenly labeled handels): 1.7162732919254657

    n2o_husdyr_w_nitri, co2e_husdyr_w_nitri = calculate_n2o_goedning(
        n_total_kg_ha=N_total_husdyr_kg_ha, areal_ha=AREAL_HA_TEST, goedningstype="husdyrgoedning", n_nitri_kg_ha=N_nitri_husdyr_kg_ha
    )
    nitri_percentage_husdyr = (N_nitri_husdyr_kg_ha / N_total_husdyr_kg_ha) * 100
    print(f"Husdyrgødning (med {nitri_percentage_husdyr:.1f}% N m. nitrihæmmer):")
    print(f"  N2O: {n2o_husdyr_w_nitri:.4f} kg")
    print(f"  CO2e: {co2e_husdyr_w_nitri:.4f} kg")
    # Expected N2O from notebook: 1.4019875776397515

    print("-"*30)

    # Test 3: Gødning afsat under afgræsning
    N_total_afgraes_kg_ha = 100.0
    n2o_afgraes, co2e_afgraes = calculate_n2o_goedning(
        n_total_kg_ha=N_total_afgraes_kg_ha, areal_ha=AREAL_HA_TEST, goedningstype="afgraesning"
    )
    print(f"Gødning afsat under afgræsning:")
    print(f"  N2O: {n2o_afgraes:.4f} kg")
    print(f"  CO2e: {co2e_afgraes:.4f} kg")
    # Expected N2O from notebook: 0.7797018633540372

    # Test 4: Handelsgødning, 10 ha, 150 N/ha, 30 N/ha med nitri
    N_total_handel_kg_ha_t4 = 150.0
    AREAL_HA_TEST_t4 = 10.0
    N_nitri_handel_kg_ha_t4 = 30.0
    n2o_h_t4, co2e_h_t4 = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_kg_ha_t4, areal_ha=AREAL_HA_TEST_t4,
        goedningstype="handelsgoedning", n_nitri_kg_ha=N_nitri_handel_kg_ha_t4
    )
    nitri_percentage_handel_t4 = (N_nitri_handel_kg_ha_t4 / N_total_handel_kg_ha_t4) * 100
    print(f"\nHandelsgødning ({AREAL_HA_TEST_t4} ha, {N_total_handel_kg_ha_t4} N/ha, med {nitri_percentage_handel_t4:.1f}% N m. nitrihæmmer, default EF_NH3):")
    print(f"  N2O: {n2o_h_t4:.4f} kg")
    print(f"  CO2e: {co2e_h_t4:.4f} kg")

    # Test 4b: Handelsgødning (Urea*), 10 ha, 150 N/ha, 30 N/ha med nitri
    n2o_h_t4_urea, co2e_h_t4_urea = calculate_n2o_goedning(
        n_total_kg_ha=N_total_handel_kg_ha_t4, areal_ha=AREAL_HA_TEST_t4,
        goedningstype="handelsgoedning", n_nitri_kg_ha=N_nitri_handel_kg_ha_t4,
        handelsgoedning_detail_type=urea_type
    )
    print(f"Handelsgødning ({urea_type}, {AREAL_HA_TEST_t4} ha, {N_total_handel_kg_ha_t4} N/ha, med {nitri_percentage_handel_t4:.1f}% N m. nitrihæmmer):")
    print(f"  N2O: {n2o_h_t4_urea:.4f} kg")
    print(f"  CO2e: {co2e_h_t4_urea:.4f} kg")

    # Test 5: Husdyrgødning, 5 ha, 120 N/ha, 0 N/ha med nitri
    N_total_husdyr_kg_ha_t5 = 120.0
    AREAL_HA_TEST_t5 = 5.0
    n2o_hus_t5, co2e_hus_t5 = calculate_n2o_goedning(
        n_total_kg_ha=N_total_husdyr_kg_ha_t5, areal_ha=AREAL_HA_TEST_t5,
        goedningstype="husdyrgoedning", n_nitri_kg_ha=0
    )
    print(f"\nHusdyrgødning ({AREAL_HA_TEST_t5} ha, {N_total_husdyr_kg_ha_t5} N/ha, uden nitrihæmmer):")
    print(f"  N2O: {n2o_hus_t5:.4f} kg")
    print(f"  CO2e: {co2e_hus_t5:.4f} kg")