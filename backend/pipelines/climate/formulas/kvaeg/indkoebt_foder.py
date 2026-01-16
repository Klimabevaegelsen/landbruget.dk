"""
Functions for calculating CO2e from purchased feed for cattle.
Based on the notebook: Kvaeg/Indkøbt foder.ipynb
"""

from typing import List, Dict, Any

def beregn_co2e_indkoebt_foder_kvaeg(fodermidler_data: List[Dict[str, Any]]) -> float:
    """
    Beregn den samlede CO2e-udledning fra indkøbt foder til kvæg.

    Args:
        fodermidler_data: En liste af dictionaries, hvor hver dictionary repræsenterer
                          et fodermiddel og skal indeholde nøglerne:
                          'maengde_kg' (float): Mængden af fodermidlet i kg.
                          'co2e_faktor_kg' (float): CO2e-emission pr. kg fodermiddel.

    Returns:
        float: Den samlede CO2e-udledning fra indkøbt foder i kg CO2e.
    """
    total_co2e = 0.0
    for foder in fodermidler_data:
        maengde_kg = foder.get('maengde_kg', 0.0)
        co2e_faktor_kg = foder.get('co2e_faktor_kg', 0.0)
        total_co2e += maengde_kg * co2e_faktor_kg
    return total_co2e