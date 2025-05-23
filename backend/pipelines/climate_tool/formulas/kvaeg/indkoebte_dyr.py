"""
Functions for calculating CO2e from purchased animals for cattle.
Based on the note with calculation basis, page 19.
"""
from typing import Dict, List, Any

# Standard live weights (kg LV) and CO2e factors (kg CO2e/kg LV)
# Based on Markdown page 19.
# Live weights for younger categories are not explicitly given and are estimated here.
# The document provides CO2e values for specific animal *weights*, so LV might be an input per batch.
# For simplicity, this example uses average LVs where not specified.
# Markdown states: "klimaværdi_ko Tung race/Jersey pr. kg LV, kg CO2e er 9,0" (for ko, kvie)
# and "4,8 kg CO2e per kg levende vægt for både tung race tyre og jersey tyre."
ANIMAL_TYPE_DATA = {
    "ko_tung":              {"default_lv_kg": 620, "co2e_per_kg_lv": 9.0},
    "ko_jersey":            {"default_lv_kg": 420, "co2e_per_kg_lv": 9.0},
    # Opdræt 0-6 mdr.
    "opdraet_0_6mdr_tung":   {"default_lv_kg": 150, "co2e_per_kg_lv": 9.0}, # Assuming LV, factor for 'kvie'
    "opdraet_0_6mdr_jersey": {"default_lv_kg": 100, "co2e_per_kg_lv": 9.0}, # Assuming LV, factor for 'kvie'
    # Tyre 0-6 mdr.
    "tyre_0_6mdr_tung":      {"default_lv_kg": 200, "co2e_per_kg_lv": 4.8}, # Assuming LV
    "tyre_0_6mdr_jersey":    {"default_lv_kg": 150, "co2e_per_kg_lv": 4.8}, # Assuming LV
    # Opdræt 6 mdr. - kælvning
    "opdraet_6mdr_kaelv_tung":   {"default_lv_kg": 550, "co2e_per_kg_lv": 9.0}, # Assuming LV, factor for 'kvie'
    "opdraet_6mdr_kaelv_jersey": {"default_lv_kg": 380, "co2e_per_kg_lv": 9.0}, # Assuming LV, factor for 'kvie'
    # Tyre 6 mdr. - slagtning
    "tyre_6mdr_slagt_tung":      {"default_lv_kg": 600, "co2e_per_kg_lv": 4.8}, # Assuming LV
    "tyre_6mdr_slagt_jersey":    {"default_lv_kg": 450, "co2e_per_kg_lv": 4.8}  # Assuming LV
}

def calculate_co2e_single_purchase_group(
    animal_type_key: str,
    number_purchased: int,
    actual_average_lv_kg: float = None  # Actual average live weight for this specific purchase group
) -> float:
    """
    Calculates CO2e for a single group of purchased cattle of the same type.
    Formula: CO2e = number_purchased * live_weight_kg * co2e_per_kg_lv_factor
    Uses actual_average_lv_kg if provided, otherwise default_lv_kg for the type.
    """
    if animal_type_key not in ANIMAL_TYPE_DATA:
        raise ValueError(f"Unknown animal type key: {animal_type_key}")

    data = ANIMAL_TYPE_DATA[animal_type_key]
    lv_kg_to_use = actual_average_lv_kg if actual_average_lv_kg is not None else data["default_lv_kg"]

    co2e_per_animal = lv_kg_to_use * data["co2e_per_kg_lv"]
    total_co2e_for_group = co2e_per_animal * number_purchased
    return total_co2e_for_group

def beregn_co2e_indkoebte_kvaeg_total(purchased_animal_groups: List[Dict[str, Any]]) -> float:
    """
    Beregn den samlede CO2e-udledning fra alle indkøbte kvæg.
    Implements formulas from Markdown page 19.

    Args:
        purchased_animal_groups: A list of dictionaries, where each dictionary represents
                                 a group of purchased animals and contains:
                                 - "animal_type_key": Key from ANIMAL_TYPE_DATA.
                                 - "number_purchased": Integer count for this group.
                                 - "actual_average_lv_kg": Optional float for the specific
                                                           average live weight of this group.
                                                           If None, standard weight is used.
    Returns:
        float: Den samlede CO2e-udledning fra alle indkøbte dyr i kg CO2e.
    """
    total_co2e_all_purchases = 0.0
    for group in purchased_animal_groups:
        total_co2e_all_purchases += calculate_co2e_single_purchase_group(
            animal_type_key=group["animal_type_key"],
            number_purchased=group["number_purchased"],
            actual_average_lv_kg=group.get("actual_average_lv_kg") # Will be None if not present
        )
    return total_co2e_all_purchases

if __name__ == "__main__":
    example_purchases_input = [
        {"animal_type_key": "ko_tung", "number_purchased": 1, "actual_average_lv_kg": 620},
        {"animal_type_key": "opdraet_0_6mdr_jersey", "number_purchased": 5}, # Uses default LV
        {"animal_type_key": "tyre_6mdr_slagt_tung", "number_purchased": 10, "actual_average_lv_kg": 580}
    ]

    total_co2e = beregn_co2e_indkoebte_kvaeg_total(example_purchases_input)
    print(f"Total CO2e from example purchased cattle: {total_co2e:.2f} kg CO2e")

    # Verification with Markdown examples (CO2_ko = klimaværdi_ko * antal kg LV)
    # Markdown page 19: "klimaværdi_ko/ tung race pr. kg LV, kg CO2e er 9,0*620 kg" -> 5580 CO2e for one 620kg tung ko
    test_ko_tung = beregn_co2e_indkoebte_kvaeg_total([
        {"animal_type_key": "ko_tung", "number_purchased": 1, "actual_average_lv_kg": 620}
    ])
    print(f"Test CO2e for one 620kg 'ko_tung': {test_ko_tung:.2f} kg CO2e (Expected: 5580.00)")

    # Markdown page 19: "klimaværdi_tyre 0-6 mdr tungrace pr. kg LV, kg CO2e er 4,8"
    # Assuming LV for "tyre_0_6mdr_tung" is e.g. 200kg (default in ANIMAL_TYPE_DATA)
    # Expected CO2e: 200kg * 4.8 CO2e/kg = 960 CO2e
    test_tyre_0_6_tung = beregn_co2e_indkoebte_kvaeg_total([
        {"animal_type_key": "tyre_0_6mdr_tung", "number_purchased": 1} # Use default weight
    ])
    expected_tyre_co2e = ANIMAL_TYPE_DATA["tyre_0_6mdr_tung"]["default_lv_kg"] * ANIMAL_TYPE_DATA["tyre_0_6mdr_tung"]["co2e_per_kg_lv"]
    print(f"Test CO2e for one default 'tyre_0_6mdr_tung' ({ANIMAL_TYPE_DATA['tyre_0_6mdr_tung']['default_lv_kg']}kg): {test_tyre_0_6_tung:.2f} kg CO2e (Expected: {expected_tyre_co2e:.2f})")