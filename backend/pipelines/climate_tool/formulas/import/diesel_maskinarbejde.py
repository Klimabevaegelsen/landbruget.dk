from typing import List, Dict, Any
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

# Attempt to load O_SCOPE1_DIESEL, similar to kvaeg_diesel.py
# In a real application, this would be a shared constant or loaded once.
if 'O_SCOPE1_DIESEL' not in globals():
    try:
        diesel_ef_data = load_json_data("tabel_36_emissioner_fra_transportsektoren_er_baseret_på_følgende_værdier_baseret_på_den_nationale_op.json")
        o_scope1_diesel_val = 0.0
        for item in diesel_ef_data.get('data_l_fuel', []):
            if item.get("Brændstoftype") == "Diesel":
                o_scope1_diesel_val = float(item.get("CO2_kg_l_fuel", 0.0))
                break
        O_SCOPE1_DIESEL = o_scope1_diesel_val
        if O_SCOPE1_DIESEL == 0.0:
             print("Warning: O_SCOPE1_DIESEL loaded as 0.0 from tabel_36 in import_diesel_maskinarbejde.py")
    except Exception as e:
        print(f"Error loading O_SCOPE1_DIESEL in import_diesel_maskinarbejde.py: {e}. Defaulting to 0.0")
        O_SCOPE1_DIESEL = 0.0 # Fallback

def beregn_diesel_fra_maskinarbejde(p_k_kr: float, p_s_kr: float, theta_m_l_pr_kr: float) -> float:
    """
    Beregner mængden af diesel fra indkøbt og solgt maskinarbejde (M).

    Args:
        p_k_kr: Prisen for maskinarbejde købt (kr).
        p_s_kr: Prisen for maskinarbejde solgt (kr).
        theta_m_l_pr_kr: Omregningsfaktor for kr maskinarbejde til L diesel (L/kr).

    Returns:
        Mængden af diesel fra maskinarbejde (L). Kan være negativ hvis mere er solgt.
    """
    m = (p_k_kr * theta_m_l_pr_kr) - (p_s_kr * theta_m_l_pr_kr)
    return m

def beregn_co2e_diesel_scope1(d_total_liter_bedrift: float, m_maskinarbejde_liter: float) -> float:
    """
    Beregner CO2e fra dieselforbrug (Scope 1 - afbrænding).

    Args:
        d_total_liter_bedrift: Det totale dieselforbrug på bedriften (L).
        m_maskinarbejde_liter: Mængden af diesel fra maskinarbejde (L) (fra beregn_diesel_fra_maskinarbejde).
        # theta_d1_kg_co2e_pr_l: Omregningsfaktor for L diesel til CO2e (kg CO2e/L) - now using global O_SCOPE1_DIESEL

    Returns:
        CO2e fra diesel scope 1 (kg CO2e).
    """
    d_scope1 = (d_total_liter_bedrift + m_maskinarbejde_liter) * O_SCOPE1_DIESEL
    return d_scope1

def beregn_co2e_diesel_scope3(d_total_liter_bedrift: float, m_maskinarbejde_liter: float, theta_d3_kg_co2e_pr_l: float) -> float:
    """
    Beregner CO2e fra dieselproduktion (Scope 3).

    Args:
        d_total_liter_bedrift: Det totale dieselforbrug på bedriften (L).
        m_maskinarbejde_liter: Mængden af diesel fra maskinarbejde (L) (fra beregn_diesel_fra_maskinarbejde).
        theta_d3_kg_co2e_pr_l: Omregningsfaktor for L dieselproduktion til CO2e (kg CO2e/L).

    Returns:
        CO2e fra diesel scope 3 (kg CO2e).
    """
    d_scope3 = (d_total_liter_bedrift + m_maskinarbejde_liter) * theta_d3_kg_co2e_pr_l
    return d_scope3

def beregn_produktaftryk_diesel_afgroede(h_a_hektar: float, t_a_typetal_diesel_pr_ha: float,
                                         alle_afgroeder_data: List[Dict[str, float]],
                                         d_total_liter_bedrift_korrigeret: float,
                                         theta_d_total_kg_co2e_pr_l: float) -> float:
    """
    Beregner produktaftrykket for diesel for en specifik afgrøde.
    P_a_diesel = (H_a * T_a) / sum(H_i * T_i) * (D_total_korrigeret) * theta_D_total

    Args:
        h_a_hektar: Det totale antal hektar på bedriften med afgrøde a.
        t_a_typetal_diesel_pr_ha: Typetal for afgrøde a's dieselforbrug (L/ha).
        alle_afgroeder_data: Liste af dictionaries, hver med 'h_i_hektar' og 't_i_typetal_diesel_pr_ha'
                             for alle afgrøder på bedriften (inkl. afgrøde a).
        d_total_liter_bedrift_korrigeret: Totalt dieselforbrug på bedriften korrigeret for maskinarbejde
                                          (D_total_liter_bedrift + m_maskinarbejde_liter) (L).
        theta_d_total_kg_co2e_pr_l: Omregningsfaktor for L diesel (forbrug + produktion) til CO2e (kg CO2e/L).
                                      This is theta_D1 + theta_D3.

    Returns:
        Produktaftryk diesel for afgrøde a (kg CO2e).
    """
    sum_h_i_t_i = 0
    for afgroede in alle_afgroeder_data:
        sum_h_i_t_i += afgroede['h_i_hektar'] * afgroede['t_i_typetal_diesel_pr_ha']

    if sum_h_i_t_i == 0: # Prevent division by zero
        return 0.0

    fordelingsnoegle = (h_a_hektar * t_a_typetal_diesel_pr_ha) / sum_h_i_t_i

    p_a_diesel_co2e = fordelingsnoegle * d_total_liter_bedrift_korrigeret * theta_d_total_kg_co2e_pr_l
    return p_a_diesel_co2e