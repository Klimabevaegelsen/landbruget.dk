import json
from pathlib import Path
from typing import Any

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

# Load EF_N2O_GENERAL from tabel_19
# This is the same factor used in marker_afgroederester.py and marker_goedning_og_nitrifikationshaemmer.py
if 'EF_N2O_GENERAL' not in globals():
    try:
        tabel_19_data = load_json_data("tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json")
        ef_n2o_general_val = 0.01 # Default
        if tabel_19_data and isinstance(tabel_19_data.get('data'), list) and len(tabel_19_data['data']) > 0:
             ef_n2o_general_val = tabel_19_data['data'][0].get('EF_N2O', 0.01)
        EF_N2O_GENERAL = float(ef_n2o_general_val)
        if EF_N2O_GENERAL == 0.0:
            print("Warning: EF_N2O_GENERAL loaded as 0.0 from tabel_19 in fjerkrae_stald.py")
    except Exception as e:
        print(f"Error loading EF_N2O_GENERAL in fjerkrae_stald.py: {e}. Defaulting to 0.01")
        EF_N2O_GENERAL = 0.01 # Fallback

def beregn_nh3_stald_fjerkrae(s_nh3: float, lambda_fjer: float, v_varmeveksler_reduktion: float = 1.0) -> float:
    """
    Beregner ammoniak (NH3) udledning fra stalden for fjerkræ.

    Args:
        s_nh3: Typetal for ammoniak udledning på stalden pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).
        v_varmeveksler_reduktion: Reduktionsfaktor for godkendt varmeveksler.
                                  0.72 if approved heat exchanger is used (28% reduction).
                                  1.0 otherwise. Defaults to 1.0.
                                  (OBS: currently only for broilers in documentation).

    Returns:
        NH3 udledning fra stald (kg NH3).
    """
    nh3_stald = (s_nh3 / lambda_fjer) * v_varmeveksler_reduktion
    return nh3_stald

def beregn_co2e_nh3_stald_fjerkrae(nh3_stald: float, theta_n2o_co2: float) -> float:
    """
    Omregner NH3 udledning fra stald (fjerkræ) til CO2e.
    Formula: NH3_stald * (44/28) * EF_N2O_GENERAL * theta_N2O_CO2

    Args:
        nh3_stald: NH3 udledning fra stald (kg NH3).
        theta_n2o_co2: Omregningsfaktor N2O til CO2.

    Returns:
        CO2e fra NH3 stald (kg CO2e).
    """
    co2e_nh3 = nh3_stald * (44.0 / 28.0) * EF_N2O_GENERAL * theta_n2o_co2
    return co2e_nh3

def beregn_n2o_stald_fjerkrae_kg_n(s_n2o_kg_n_potential: float, lambda_fjer: float) -> float:
    """
    Beregner lattergas (N2O) udledning fra stalden for fjerkræ, som kg N.

    Args:
        s_n2o_kg_n_potential: Typetal for lattergas udledning (kg N der bliver til N2O) på stalden pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).

    Returns:
        N2O udledning fra stald (kg N der bliver til N2O).
    """
    n2o_stald_kg_n = s_n2o_kg_n_potential / lambda_fjer
    return n2o_stald_kg_n

def beregn_co2e_n2o_stald_fjerkrae(n2o_stald_kg_n: float, theta_n2o_co2: float) -> float:
    """
    Omregner N2O udledning fra stald (fjerkræ) til CO2e.
    Input N2O er i kg N.

    Args:
        n2o_stald_kg_n: N2O udledning fra stald, expressed as kg N (kg N that becomes N2O).
        theta_n2o_co2: Omregningsfaktor N2O til CO2 (GWP-værdi for N2O).

    Returns:
        CO2e fra N2O stald (kg CO2e).
    """
    kg_n2o = n2o_stald_kg_n * (44.0 / 28.0) # Convert kg N to kg N2O
    co2e_n2o = kg_n2o * theta_n2o_co2
    return co2e_n2o

def beregn_ch4_stald_fjerkrae(s_ch4: float, lambda_fjer: float) -> float:
    """
    Beregner metan (CH4) udledning fra stalden for fjerkræ.

    Args:
        s_ch4: Typetal for metan udledning på stalden pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).

    Returns:
        CH4 udledning fra stald (kg CH4).
    """
    ch4_stald = s_ch4 / lambda_fjer
    return ch4_stald

def beregn_co2e_ch4_stald_fjerkrae(ch4_stald: float, theta_ch4_co2: float) -> float:
    """
    Omregner CH4 udledning fra stald (fjerkræ) til CO2e.

    Args:
        ch4_stald: CH4 udledning fra stald (kg CH4).
        theta_ch4_co2: Omregningsfaktor CH4 til CO2.

    Returns:
        CO2e fra CH4 stald (kg CO2e).
    """
    co2e_ch4 = ch4_stald * theta_ch4_co2
    return co2e_ch4