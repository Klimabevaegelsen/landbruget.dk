import json
from pathlib import Path
from typing import Any, Tuple

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

def get_enteric_ch4_factors_from_table(fjerkrae_type_key: str) -> Tuple[float, float]:
    """Loads CH4_EF and Antal_dyr (for lambda_fjer) from tabel_8 based on fjerkrae_type_key."""
    data = load_json_data("tabel_8_faktorer_til_beregning_af_metan_emission_fra_fordøjelse_hos_fjerkræ_kg_ch4_per_100_eller_100.json")
    default_ef = 0.0
    default_lambda = 1.0 # Avoid division by zero if not found, though logic should handle this

    for item in data.get('data', []):
        if item.get("Fjerkrætype_Dage") == fjerkrae_type_key:
            ch4_ef_str = str(item.get("CH4_EF", "0.0")).replace("*", "") # Remove asterisk if present
            try:
                ef_ch4 = float(ch4_ef_str)
                lambda_val = float(item.get("Antal_dyr", default_lambda))
                return ef_ch4, lambda_val
            except ValueError:
                print(f"Warning: Could not parse float for CH4_EF '{ch4_ef_str}' for {fjerkrae_type_key}. Using defaults.")
                return default_ef, default_lambda

    print(f"Warning: Fjerkrætype_Dage '{fjerkrae_type_key}' not found in tabel_8. Using defaults for CH4_EF and lambda.")
    return default_ef, default_lambda

def beregn_ch4_enterisk_fjerkrae(fjerkrae_type_key: str) -> float:
    """
    Beregner CH4 fra fordøjelsen for fjerkræ based on type from tabel_8.

    Args:
        fjerkrae_type_key: String key matching "Fjerkrætype_Dage" in tabel_8.
                           Example: "Høner", "Slagtekyllinger: 35 dage"

    Returns:
        CH4 fra fordøjelsen (kg CH4 pr. individuelt dyr).
    """
    e_ch4_table, lambda_fjer_table = get_enteric_ch4_factors_from_table(fjerkrae_type_key)

    if lambda_fjer_table == 0:
        print(f"Warning: lambda_fjer is 0 for {fjerkrae_type_key}, cannot calculate CH4. Returning 0.")
        return 0.0

    ch4_enterisk_pr_individ = e_ch4_table / lambda_fjer_table # EF is per lambda_fjer animals
    return ch4_enterisk_pr_individ

def beregn_co2e_enterisk_fjerkrae(ch4_enterisk_pr_individ: float, theta_ch4_co2: float) -> float:
    """
    Omregner enterisk metan fra fjerkræ til CO2e.

    Args:
        ch4_enterisk_pr_individ: CH4 fra fordøjelsen (kg CH4 pr. individuelt dyr).
        theta_ch4_co2: Omregningsfaktor CH4 til CO2.

    Returns:
        CO2e fra enterisk metan (kg CO2e pr. individuelt dyr).
    """
    co2e = ch4_enterisk_pr_individ * theta_ch4_co2
    return co2e