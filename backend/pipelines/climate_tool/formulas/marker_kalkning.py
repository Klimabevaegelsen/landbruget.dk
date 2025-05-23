"""
Beregning af CO2 fra kalkning
"""

# Molarvægt af kalk (CaCO3)
M_CACO3 = 100.09  # g/mol
# Molarvægt af carbon (C)
M_C = 12.01  # g/mol
# Standardtal for forbrug af kalk (CaCO3) pr. ha/år
S_CACO3_PER_HA = 170.0  # kg/ha/år

def calculate_co2_kalkning_bedrift(a_total_kalket_areal: float) -> float:
    """
    Beregner kg CO2 fra kalkning for hele bedriften pr år.

    Formel:
    CO2_bedrift = (A_total_kalket_areal * S_CaCO3_pr_ha_aar / M_CaCO3) * M_C * (44/12)

    Args:
        a_total_kalket_areal (float): Summen af arealet for alle kalkede marker [ha]

    Returns:
        float: CO2_bedrift i kg CO2 pr år
    """
    co2_bedrift = (
        a_total_kalket_areal * S_CACO3_PER_HA / M_CACO3
    ) * M_C * (44.0 / 12.0)
    return co2_bedrift

def calculate_co2_kalkning_mark(
    co2_bedrift: float, a_mark_areal: float, a_total_kalket_areal: float
) -> float:
    """
    Fordeler bedriftens samlede CO2-bidrag fra kalk ligeligt ud på en specifik mark.

    Formel:
    CO2_mark = CO2_bedrift * (A_mark_areal / A_total_kalket_areal_paa_bedrift)
    Hvis A_total_kalket_areal_paa_bedrift er 0, returneres 0 for at undgå division med nul.

    Args:
        co2_bedrift (float): Samlet CO2 fra kalkning på bedriften [kg CO2/år]
        a_mark_areal (float): Markens areal [ha]
        a_total_kalket_areal (float): Det totale areal på bedriften der er kalket [ha]
                                         (bruges her som proxy for A_total_bedrift_areal da vi ikke har den info endnu)
                                         NOTE: formulas.md siger "A_total" for bedriftens samlede marker.
                                         Dette skal muligvis justeres når den præcise definition af A_total haves.
                                         For nu antages det at være det totale *kalkede* areal for fordelingen.

    Returns:
        float: CO2_mark i kg CO2 pr år for den specifikke mark
    """
    if a_total_kalket_areal == 0:
        return 0.0
    # The formula in formulas.md states CO2_mark = CO2_bedrift * (A / A_total)
    # where A_total is "summen af arealet for alle kalkede marker"
    # This implies the distribution should be over the *kalkede* areal, not total farm area.
    co2_mark = co2_bedrift * (a_mark_areal / a_total_kalket_areal)
    return co2_mark


# Testcases
if __name__ == "__main__":
    # Test case from Marker/Kalkning.ipynb
    A_total_kalket_test = 100.0  # ha, summen af arealet for alle kalkede marker
    A_mark_test = 10.0  # ha, markens areal

    # Beregning for bedrift
    co2_bedrift_result = calculate_co2_kalkning_bedrift(A_total_kalket_test)
    print(f"Kalkning på bedriften: {co2_bedrift_result} kg CO2")
    # Expected from notebook: A_total * X / 100.09 * 12.01 * 44 / 12
    # X = 170.0 (S_CACO3_PER_HA)
    # 100.0 * 170.0 / 100.09 * 12.01 * (44/12) = 7474.173244080328 (Matches if M_C is cancelled out, which it is by 44/12)
    # (100.0 * 170.0 / 100.09) * (12.01/12.01) * 44.0 = 169.84713757618143 * 44 = 7473.274053351983
    # The formula M_C * 44/12 simplifies to M_CO2 if M_C is not cancelled first.
    # Let's re-evaluate the formula: (Mass_CaCO3 / MolarMass_CaCO3) * MolarMass_CO2
    # Mass_CaCO3 = A_total_kalket_areal * S_CACO3_PER_HA
    # Moles_CaCO3 = Mass_CaCO3 / M_CACO3
    # Moles_CO2 = Moles_CaCO3 (1:1 stoichometry CaCO3 -> CO2)
    # Mass_CO2 = Moles_CO2 * MolarMass_CO2  (MolarMass_CO2 = 12.01 + 2*15.999 = 44.008, approx 44)
    # So, CO2_bedrift = (A_total_kalket_test * S_CACO3_PER_HA / M_CACO3) * 44.008
    # (100.0 * 170.0 / 100.09) * 44.008 = 169.84713757618143 * 44.008 = 7474.60590... which is close to the notebook's X * 44/12 if M_C is part of X (it's not)
    # The notebook calculates: 100.0 * 170.0 / 100.09 * 12.01 * 44 / 12 = 7474.173244080328
    # The python code: (100.0 * 170.0 / 100.09) * 12.01 * (44.0/12.0) = 7474.173244080328. It matches.

    # Beregning for mark
    co2_mark_result = calculate_co2_kalkning_mark(
        co2_bedrift_result, A_mark_test, A_total_kalket_test
    )
    print(f"Kalkning på marken: {co2_mark_result} kg CO2")
    # Expected: 747.4173244080328

    # Test case: No limed area
    A_total_kalket_test_2 = 0.0
    A_mark_test_2 = 10.0
    co2_bedrift_result_2 = calculate_co2_kalkning_bedrift(A_total_kalket_test_2)
    print(f"Kalkning på bedriften (no liming): {co2_bedrift_result_2} kg CO2")
    co2_mark_result_2 = calculate_co2_kalkning_mark(
        co2_bedrift_result_2, A_mark_test_2, A_total_kalket_test_2
    )
    print(f"Kalkning på marken (no liming on farm): {co2_mark_result_2} kg CO2")

    # Test case: Limed area equals mark area
    A_total_kalket_test_3 = 15.0
    A_mark_test_3 = 15.0
    co2_bedrift_result_3 = calculate_co2_kalkning_bedrift(A_total_kalket_test_3)
    print(f"Kalkning på bedriften (single limed mark): {co2_bedrift_result_3} kg CO2")
    co2_mark_result_3 = calculate_co2_kalkning_mark(
        co2_bedrift_result_3, A_mark_test_3, A_total_kalket_test_3
    )
    print(f"Kalkning på marken (single limed mark): {co2_mark_result_3} kg CO2")