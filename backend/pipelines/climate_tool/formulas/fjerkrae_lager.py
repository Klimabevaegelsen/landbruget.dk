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
if 'EF_N2O_GENERAL' not in globals():
    try:
        tabel_19_data = load_json_data("tabel_19_ammoniak-emissionerne_fra_udbringning_af_organisk_gødning_side_75-76.json")
        ef_n2o_general_val = 0.01 # Default
        if tabel_19_data and isinstance(tabel_19_data.get('data'), list) and len(tabel_19_data['data']) > 0:
             ef_n2o_general_val = tabel_19_data['data'][0].get('EF_N2O', 0.01)
        EF_N2O_GENERAL = float(ef_n2o_general_val)
        if EF_N2O_GENERAL == 0.0:
            print("Warning: EF_N2O_GENERAL loaded as 0.0 from tabel_19 in fjerkrae_lager.py")
    except Exception as e:
        print(f"Error loading EF_N2O_GENERAL in fjerkrae_lager.py: {e}. Defaulting to 0.01")
        EF_N2O_GENERAL = 0.01 # Fallback

def beregn_nh3_lager_fjerkrae(l_nh3: float, lambda_fjer: float, d: float, f_fast_laag_reduktion: float = 1.0) -> float:
    """
    Beregner ammoniak (NH3) udledning fra lageret for fjerkræ.

    Args:
        l_nh3: Typetal for ammoniak udledning på lageret pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).
        d: Andelen af dybstrøelse kørt på lager (%).
        f_fast_laag_reduktion: Reduktionsfaktor for fast låg på gylletank.
                                 1.0 if no lid or not applicable.
                                 0.69 if lid and deep litter + slurry (31% reduction).
                                 0.50 if lid and pure slurry (50% reduction).
                                 Defaults to 1.0.

    Returns:
        NH3 udledning fra lager (kg NH3).
    """
    nh3_lager = (l_nh3 / lambda_fjer) * d * f_fast_laag_reduktion
    return nh3_lager

def beregn_co2e_nh3_lager_fjerkrae(nh3_lager: float, theta_n2o_co2: float) -> float:
    """
    Omregner NH3 udledning fra lager (fjerkræ) til CO2e.
    Formula: NH3_lager * (44/28) * EF_N2O_GENERAL * theta_N2O_CO2

    Args:
        nh3_lager: NH3 udledning fra lager (kg NH3).
        theta_n2o_co2: Omregningsfaktor N2O til CO2.

    Returns:
        CO2e fra NH3 lager (kg CO2e).
    """
    co2e_nh3 = nh3_lager * (44.0 / 28.0) * EF_N2O_GENERAL * theta_n2o_co2
    return co2e_nh3

def beregn_n2o_lager_fjerkrae(l_n2o_kg_N_potential: float, lambda_fjer: float, d: float) -> float:
    """
    Beregner lattergas (N2O) udledning fra lageret for fjerkræ, som kg N.

    Args:
        l_n2o_kg_N_potential: Typetal for lattergas udledning (kg N der bliver til N2O) på lageret pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).
        d: Andelen af dybstrøelse kørt på lager (%).

    Returns:
        N2O udledning fra lager (kg N der bliver til N2O).
    """
    n2o_lager_kg_n = (l_n2o_kg_N_potential / lambda_fjer) * d
    return n2o_lager_kg_n

def beregn_co2e_n2o_lager_fjerkrae(n2o_lager_kg_n: float, theta_n2o_co2: float) -> float:
    """
    Omregner N2O udledning fra lager (fjerkræ) til CO2e.
    Input N2O er i kg N.

    Args:
        n2o_lager_kg_n: N2O udledning fra lager, expressed as kg N (kg N that becomes N2O).
        theta_n2o_co2: Omregningsfaktor N2O til CO2 (GWP-værdi for N2O).

    Returns:
        CO2e fra N2O lager (kg CO2e).
    """
    # Konverter kg N til kg N2O: n2o_lager_kg_n * (Molekylvægt N2O / Molekylvægt N2)
    # N2O = 44.013 g/mol, N = 14.0067 g/mol. For N2 in N2O, it's 2*N = 28.0134
    # Faktor = 44.013 / 28.0134 approx 44/28
    kg_n2o = n2o_lager_kg_n * (44.0 / 28.0)
    co2e_n2o = kg_n2o * theta_n2o_co2
    return co2e_n2o

def beregn_ch4_lager_fjerkrae(l_ch4: float, lambda_fjer: float, d: float) -> float:
    """
    Beregner metan (CH4) udledning fra lageret for fjerkræ.

    Args:
        l_ch4: Typetal for metan udledning på lageret pr dyre/staldtype.
        lambda_fjer: Typetal (1000.0 for slagtekyllinger, 100.0 for hønniker/høns).
        d: Andelen af dybstrøelse kørt på lager (%).

    Returns:
        CH4 udledning fra lager (kg CH4).
    """
    ch4_lager = (l_ch4 / lambda_fjer) * d
    return ch4_lager

def beregn_co2e_ch4_lager_fjerkrae(ch4_lager: float, theta_ch4_co2: float) -> float:
    """
    Omregner CH4 udledning fra lager (fjerkræ) til CO2e.

    Args:
        ch4_lager: CH4 udledning fra lager (kg CH4).
        theta_ch4_co2: Omregningsfaktor CH4 til CO2.

    Returns:
        CO2e fra CH4 lager (kg CO2e).
    """
    co2e_ch4 = ch4_lager * theta_ch4_co2
    return co2e_ch4