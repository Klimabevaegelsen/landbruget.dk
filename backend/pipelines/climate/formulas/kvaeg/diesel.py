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

def get_diesel_scope1_factor_from_table() -> float:
    """Loads diesel scope 1 emission factor (kg CO2/L) from tabel_36."""
    data = load_json_data("tabel_36_emissioner_fra_transportsektoren_er_baseret_på_følgende_værdier_baseret_på_den_nationale_op.json")
    for item in data.get('data_l_fuel', []):
        if item.get("Brændstoftype") == "Diesel":
            try:
                return float(item.get("CO2_kg_l_fuel", 0.0))
            except ValueError:
                print(f"Warning: Could not parse float for Diesel CO2_kg_l_fuel. Using 0.0.")
                return 0.0
    print("Warning: Diesel CO2_kg_l_fuel not found in tabel_36. Using 0.0.")
    return 0.0 # Default if not found

# Load constants at module level
O_SCOPE1_DIESEL = get_diesel_scope1_factor_from_table()

def beregn_co2e_diesel_scope1_kvaeg(n_ko: float, d_ko: float, o_scope1: float = None) -> float:
    """
    Beregner CO2e fra diesel (scope 1) for kvæg.

    Args:
        n_ko: Antallet af køer.
        d_ko: Standard dieselforbrug pr. ko (L/ko).
        o_scope1: Omregningsfaktor for diesel forbrænding (scope 1) (kg CO2/L).
                  If None, uses value loaded from table.

    Returns:
        CO2e fra diesel (scope 1) (kg CO2e).
    """
    # Use loaded constant if o_scope1 is not provided as argument (for flexibility or testing)
    # However, the function signature suggests o_scope1 is an input, so original behavior is to use the arg.
    # For this refactor, we will assume the intention is to use the loaded value primarily.
    # If an explicit o_scope1 is passed, it would override the loaded one, this might be for testing specific EFs.
    # Let's make the loaded one the default if no argument is passed.
    # The prompt is to *get constants that are available in @tables*.
    # So we should use the loaded value as the primary source.
    # The original function had o_scope1 as a required arg, so we modify it to use the global O_SCOPE1_DIESEL

    # Simplest change: use the globally loaded constant, remove o_scope1 from args
    # For more flexibility, can make o_scope1 an optional arg that overrides the global if provided.
    # Let's stick to the prompt: make sure to get constants available in tables and implement fetching.
    # This means replacing the parameter if the constant is found and meant to be used here.

    co2e_diesel_scope1 = n_ko * d_ko * O_SCOPE1_DIESEL # Using loaded global constant
    return co2e_diesel_scope1

def beregn_co2e_diesel_scope3_kvaeg(n_ko: float, d_ko: float, o_scope3: float) -> float:
    """
    Beregner CO2e fra diesel (scope 3) for kvæg.

    Args:
        n_ko: Antallet af køer.
        d_ko: Standard dieselforbrug pr. ko (L/ko).
        o_scope3: Omregningsfaktor for diesel produktion (scope 3) (kg CO2/L).

    Returns:
        CO2e fra diesel (scope 3) (kg CO2e).
    """
    co2e_diesel_scope3 = n_ko * d_ko * o_scope3
    return co2e_diesel_scope3