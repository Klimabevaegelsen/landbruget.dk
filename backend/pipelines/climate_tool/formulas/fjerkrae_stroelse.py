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

def get_straw_co2e_factor_from_table() -> float:
    """Loads straw CO2e factor (kg CO2e/ton) from tabel_34."""
    data = load_json_data("tabel_34_klimaaftryk_for_1_ton_hvedehalm_side_107.json")
    for item in data.get('data', []):
        if item.get("Aktivitet") == "Samlet":
            footprint_str = item.get("Klimaaftryk_pr_ton", "0.0").replace(" kg CO2e", "").replace(",", ".")
            try:
                return float(footprint_str)
            except ValueError:
                print(f"Warning: Could not parse float for Straw CO2e factor. Using 0.0.")
                return 0.0
    print("Warning: Straw CO2e factor not found in tabel_34. Using 0.0.")
    return 0.0

THETA_H_KG_CO2E_PR_TON = get_straw_co2e_factor_from_table()

def beregn_co2e_stroelse_scope1(v_sp_ton: float, theta_sp1_kg_co2e_pr_ton: float, a_fjer: float) -> float:
    """
    Beregner CO2e fra strøelse (scope 1 - spagnum nedbrydning) for fjerkræ.

    Args:
        v_sp_ton: Mængden af spagnum (ton).
        theta_sp1_kg_co2e_pr_ton: Omregningsfaktor for spagnum nedbrydning (kg CO2e/ton).
        a_fjer: Det totale antal producerede fjerkræ som strøelsen skal fordeles på.

    Returns:
        CO2e fra strøelse scope 1 (kg CO2e pr fjerkræ).
    """
    if a_fjer == 0: # Prevent division by zero
        return 0.0
    co2e_scope1 = (v_sp_ton * theta_sp1_kg_co2e_pr_ton) / a_fjer
    return co2e_scope1

def beregn_theta_sp3_spagnum_produktion_transport(theta_ch4_co2: float, theta_n2o_co2: float) -> float:
    """
    Beregner omregningsfaktor for spagnum produktion og transport (theta_sp3).
    Formula: 154 + (199/1000) * theta_CH4_CO2 + (14/1000) * theta_N2O_CO2

    Args:
        theta_ch4_co2: Omregningsfaktor CH4 til CO2 (GWP-værdi).
        theta_n2o_co2: Omregningsfaktor N2O til CO2 (GWP-værdi).

    Returns:
        theta_sp3 (kg CO2e/ton spagnum).
    """
    theta_sp3 = 154 + (199.0 / 1000.0) * theta_ch4_co2 + (14.0 / 1000.0) * theta_n2o_co2
    return theta_sp3

def beregn_co2e_stroelse_scope3(v_h_ton: float,
                                v_s_ton: float, theta_s_kg_co2e_pr_ton: float,
                                v_sp_ton: float, theta_sp3_kg_co2e_pr_ton: float,
                                a_fjer: float) -> float:
    """
    Beregner CO2e fra strøelse (scope 3 - produktion/transport) for fjerkræ.

    Args:
        v_h_ton: Mængden af halm (ton).
        v_s_ton: Mængden af spåner/savsmuld (ton).
        theta_s_kg_co2e_pr_ton: Omregningsfaktor for spåner/savsmuld (kg CO2e/ton).
        v_sp_ton: Mængden af spagnum (ton).
        theta_sp3_kg_co2e_pr_ton: Omregningsfaktor for spagnum produktion og transport (kg CO2e/ton).
                                    (Kan beregnes med beregn_theta_sp3_spagnum_produktion_transport)
        a_fjer: Det totale antal producerede fjerkræ som strøelsen skal fordeles på.

    Returns:
        CO2e fra strøelse scope 3 (kg CO2e pr fjerkræ).
    """
    if a_fjer == 0: # Prevent division by zero
        return 0.0

    co2e_halm = v_h_ton * THETA_H_KG_CO2E_PR_TON
    co2e_spaaner = v_s_ton * theta_s_kg_co2e_pr_ton
    co2e_spagnum = v_sp_ton * theta_sp3_kg_co2e_pr_ton

    co2e_scope3 = (co2e_halm + co2e_spaaner + co2e_spagnum) / a_fjer
    return co2e_scope3