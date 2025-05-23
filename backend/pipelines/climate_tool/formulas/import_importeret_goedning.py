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

def get_fertilizer_constants_from_table():
    """Loads N, P, K constants from gødning_carbon_footprint_side_82.json"""
    data = load_json_data("gødning_carbon_footprint_side_82.json")
    constants = {}
    for item in data.get('data', []):
        mineral = item.get("Mineral_Kg")
        # Extract numeric part of the footprint string, handling comma as decimal separator
        footprint_str = item.get("Carbon_Footprint_CO2_aekv_per_kg", "0.0").split(' ')[0].replace(',', '.')
        try:
            value = float(footprint_str)
        except ValueError:
            print(f"Warning: Could not parse float from '{footprint_str}' for {mineral}. Using 0.0.")
            value = 0.0

        if "Kvælstof (N)" in mineral:
            constants['nk'] = value
        elif "Fosfor (P)" in mineral:
            constants['pk'] = value
        elif "Kalium (K)" in mineral:
            constants['kk'] = value
    return constants.get('nk', 0.0), constants.get('pk', 0.0), constants.get('kk', 0.0) # Return defaults if not found

# Load constants at module level
NK_KONSTANT, PK_KONSTANT, KK_KONSTANT = get_fertilizer_constants_from_table()

def beregn_co2e_importeret_goedning(n_total: float, p_total: float, k_total: float, areal: float) -> float:
    """
    Beregner kg CO2e fra importeret handelsgødning pr. mark.

    Args:
        n_total: Sum af kg N fra alle udbringninger af handelsgødning (kg/ha).
        p_total: Sum af kg P fra alle udbringninger af handelsgødning (kg/ha).
        k_total: Sum af kg K fra alle udbringninger af handelsgødning (kg/ha).
        areal: Markens areal (ha).

    Returns:
        Kg CO2e fra importeret gødning.
    """
    # Use the loaded constants
    co2e = (n_total * NK_KONSTANT + p_total * PK_KONSTANT + k_total * KK_KONSTANT) * areal
    return co2e