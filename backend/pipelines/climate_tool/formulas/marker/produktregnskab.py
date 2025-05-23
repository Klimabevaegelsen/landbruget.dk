"""
Beregning af produktaftryk for en specifik afgrøde.

Dette modul aggregerer CO2e-bidrag fra forskellige emissionskilder på tværs
af alle marker, hvor en bestemt afgrøde dyrkes, for at beregne et gennemsnitligt
produktaftryk pr. hektar for den pågældende afgrøde.
"""

from typing import List, Dict, Any, Callable

# Define a type alias for a field data structure for clarity
# Each field would have its area and CO2e contributions from various sources.
FieldData = Dict[str, Any]

# Define a type alias for CO2e calculation functions from other modules
# These would typically be imported from their respective .py files
# For example: from .marker_kalkning import calculate_co2e_kalkning_mark_total
# CO2eCalcFunction = Callable[..., float] # Takes field specific params, returns kg CO2e for that source on that field


def calculate_produktaftryk_afgroede_kg_co2e_pr_ha(
    fields_data: List[FieldData],
    target_afgroede_navn: str,
    # Functions from other modules would be passed or imported here
    # e.g.: kalkning_func: CO2eCalcFunction,
    #       goedning_func: CO2eCalcFunction,
    #       ... etc.
) -> float:
    """
    Beregner produktaftrykket (P_a) for en given afgrøde (target_afgroede_navn)
    baseret på en liste af marker (fields_data).

    P_a = Sum(CO2e_total_for_field_j_with_crop_a) / Sum(Area_of_field_j_with_crop_a)

    Args:
        fields_data (List[FieldData]): En liste af dictionaries, hvor hver dictionary
            repræsenterer en mark og indeholder mindst:
            - 'afgroede_navn': Navnet på afgrøden på marken (str)
            - 'areal_ha': Markens areal i hektar (float)
            - 'co2e_sources': En dictionary med CO2e-bidrag (i kg CO2e)
                              fra forskellige kilder for den mark, f.eks.:
                              {'kalkning': 150.0, 'goedning': 2000.0, ...}
                              Alternatively, it could contain raw inputs for each
                              formula function to calculate these on the fly.
        target_afgroede_navn (str): Navnet på den afgrøde, som produktaftrykket
                                     skal beregnes for.

    Returns:
        float: Produktaftrykket for afgrøden i kg CO2e pr. ha.
               Returnerer 0.0 hvis ingen marker med den specificerede afgrøde findes,
               eller hvis det totale areal er nul.
    """
    total_co2e_for_target_afgroede = 0.0
    total_areal_for_target_afgroede = 0.0

    for field in fields_data:
        if field.get('afgroede_navn') == target_afgroede_navn:
            areal_ha = field.get('areal_ha', 0.0)
            if areal_ha <= 0: # Skip fields with no or invalid area
                continue

            # Sum CO2e from all sources for this field
            # This assumes 'co2e_sources' dictionary is provided with pre-calculated values
            field_total_co2e = sum(field.get('co2e_sources', {}).values())

            # --- Illustrative alternative: Calculating CO2e on the fly ---
            # field_total_co2e = 0
            # field_total_co2e += kalkning_func(field.get('kalkning_input_1'), ...)
            # field_total_co2e += goedning_func(field.get('goedning_input_1'), ...)
            # ... and so on for all relevant emission source functions
            # This would require passing the actual calculation functions and their specific inputs.
            # For simplicity in this example, we assume pre-summed 'co2e_sources'.
            # --------------------------------------------------------------

            total_co2e_for_target_afgroede += field_total_co2e
            total_areal_for_target_afgroede += areal_ha

    if total_areal_for_target_afgroede == 0:
        return 0.0  # Undgå division med nul

    produktaftryk = total_co2e_for_target_afgroede / total_areal_for_target_afgroede
    return produktaftryk


# Testcases
if __name__ == "__main__":
    # Eksempel data for marker (simulerer data der ville komme fra en database/anden kilde)
    # CO2e values are illustrative totals for each field from relevant sources.
    sample_fields_data: List[FieldData] = [
        {
            'mark_id': 1,
            'afgroede_navn': "Vårbyg",
            'areal_ha': 40.0,
            'co2e_sources': {
                'goedning': 40 * 150.0,    # 150 kg CO2e/ha from goedning
                'kalkning': 40 * 10.0,     # 10 kg CO2e/ha from kalkning
                'afgroederester': 40 * 50.0, # 50 kg CO2e/ha from afgroederester
                'nitratudvaskning': 40 * 20.0, # etc.
                'organogene_jorde': 0.0, # Assume no organogene jorde for this field
                # 'import_goedning': 40 * 5.0, # Illustrative import emission
                # 'import_diesel': 40 * 15.0,  # Illustrative import emission
            }
        },
        {
            'mark_id': 2,
            'afgroede_navn': "Vårbyg",
            'areal_ha': 180.0,
            'co2e_sources': {
                'goedning': 180 * 160.0,
                'kalkning': 180 * 12.0,
                'afgroederester': 180 * 55.0,
                'nitratudvaskning': 180 * 22.0,
                'organogene_jorde': 180 * 30.0, # This field has organogene jorde
            }
        },
        {
            'mark_id': 3,
            'afgroede_navn': "Vårbyg",
            'areal_ha': 80.0,
            'co2e_sources': {
                'goedning': 80 * 140.0,
                'kalkning': 80 * 9.0,
                'afgroederester': 80 * 45.0,
                'nitratudvaskning': 80 * 18.0,
            }
        },
        {
            'mark_id': 4,
            'afgroede_navn': "Vinterhvede",
            'areal_ha': 100.0,
            'co2e_sources': {
                'goedning': 100 * 200.0,
                'kalkning': 100 * 15.0,
                'afgroederester': 100 * 60.0,
                'nitratudvaskning': 100 * 25.0,
            }
        },
        {
            'mark_id': 5,
            'afgroede_navn': "Rajgræs",
            'areal_ha': 0.0, # Invalid area, should be skipped
            'co2e_sources': {'goedning': 500.0}
        },
         {
            'mark_id': 6,
            'afgroede_navn': "Vårbyg", # Field with no co2e sources listed
            'areal_ha': 20.0,
            'co2e_sources': {}
        },
    ]

    # Beregn produktaftryk for Vårbyg
    pa_vaarbyg = calculate_produktaftryk_afgroede_kg_co2e_pr_ha(
        sample_fields_data, "Vårbyg"
    )
    print(f"Produktaftryk for Vårbyg: {pa_vaarbyg:.2f} kg CO2e/ha")

    # Beregn produktaftryk for Vinterhvede
    pa_vinterhvede = calculate_produktaftryk_afgroede_kg_co2e_pr_ha(
        sample_fields_data, "Vinterhvede"
    )
    print(f"Produktaftryk for Vinterhvede: {pa_vinterhvede:.2f} kg CO2e/ha")

    # Beregn produktaftryk for en afgrøde der ikke findes
    pa_ukendt = calculate_produktaftryk_afgroede_kg_co2e_pr_ha(
        sample_fields_data, "Majs"
    )
    print(f"Produktaftryk for Majs: {pa_ukendt:.2f} kg CO2e/ha")

    # Eksempel på forventet beregning for Vårbyg:
    # Mark 1 (Vårbyg, 40 ha):
    #   CO2e = 40*(150+10+50+20) = 40 * 230 = 9200 kg
    # Mark 2 (Vårbyg, 180 ha):
    #   CO2e = 180*(160+12+55+22+30) = 180 * 279 = 50220 kg
    # Mark 3 (Vårbyg, 80 ha):
    #   CO2e = 80*(140+9+45+18) = 80 * 212 = 16960 kg
    # Mark 6 (Vårbyg, 20 ha, no sources):
    #   CO2e = 0 kg
    # Total CO2e Vårbyg = 9200 + 50220 + 16960 + 0 = 76380 kg
    # Total Areal Vårbyg = 40 + 180 + 80 + 20 = 320 ha
    # P_Vårbyg = 76380 / 320 = 238.6875 kg CO2e/ha
    print(f"Forventet P_Vårbyg manuel beregning: {76380 / 320:.2f} kg CO2e/ha")

    # Eksempel på forventet beregning for Vinterhvede:
    # Mark 4 (Vinterhvede, 100 ha):
    #   CO2e = 100*(200+15+60+25) = 100 * 300 = 30000 kg
    # Total CO2e Vinterhvede = 30000 kg
    # Total Areal Vinterhvede = 100 ha
    # P_Vinterhvede = 30000 / 100 = 300.00 kg CO2e/ha
    print(f"Forventet P_Vinterhvede manuel beregning: {30000 / 100:.2f} kg CO2e/ha")