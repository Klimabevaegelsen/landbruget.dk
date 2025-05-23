"""
Beregning af kg N2O fra nitratudvaskning
"""

# Formel:
# N2O_nitratudvaskning = T * 0.0075 * (44/28) * H

# Hvor:
# T: typetal for nitratudvaskning = Tabelværdi
# H: Antal ha på marken = fra MO eller brugerinput [ha]

# Omregning til CO2e:
# CO2e = N2O_nitratudvaskning * theta_N2O_CO2

# Hvor:
# theta_N2O_CO2: Omregningsfaktor N2O til CO2 = Tabelværdi

THETA_N2O_CO2 = 265.0  # Omregningsfaktor N2O til CO2

def calculate_n2o_nitratudvaskning(t: float, h: float) -> tuple[float, float]:
    """
    Calculates N2O from nitratudvaskning and its CO2e value.

    Args:
        t (float): Typetal for nitratudvaskning
        h (float): Antal ha på marken [ha]

    Returns:
        tuple[float, float]: N2O_nitratudvaskning in kg N2O, CO2e in kg
    """
    n2o_nitratudvaskning = t * 0.0075 * (44.0 / 28.0) * h
    co2e = n2o_nitratudvaskning * THETA_N2O_CO2
    return n2o_nitratudvaskning, co2e


# Testcases
if __name__ == "__main__":
    # Test case 1: Vårbyg (afgrødekode 1)
    T_vaar = 63.0  # typetal for vårbyg
    H_test_1 = 100.0
    n2o_1, co2e_1 = calculate_n2o_nitratudvaskning(T_vaar, H_test_1)
    print(f"Test Case 1 (Vårbyg):")
    print(f"  Nitratudvaskning: {n2o_1} kg N2O")
    print(f"  Omregnet til CO2e: {co2e_1} kg CO2e")
    # Expected N2O: 74.25 kg, Expected CO2e: 19676.25 kg (using 265) - formula.md has 22126.5, typo in doc or constant?
    # The formula.md screenshot shows 22126.5, which is 74.25 * 298, not 265. However, the constant defined in the notebook is 265.
    # Will stick to the constant defined in the notebook cell: theta_N2O_CO2 = 265.0
    # Re-calculating expected with 265: 74.25 * 265 = 19676.25

    # Test case 2: Solsikke (afgrødekode 24)
    T_sol = 74.0  # typetal for solsikke
    H_test_2 = 100.0
    n2o_2, co2e_2 = calculate_n2o_nitratudvaskning(T_sol, H_test_2)
    print(f"\nTest Case 2 (Solsikke):")
    print(f"  Nitratudvaskning: {n2o_2} kg N2O")
    print(f"  Omregnet til CO2e: {co2e_2} kg CO2e")
    # Expected N2O: 87.21428571428571 kg, Expected CO2e: 23111.78571428571 kg (using 265)
    # formulas.md output: 25989.85714285714, which is 87.21428571428571 * 298. Again, discrepancy.

    # Test case 3: Vårbyg (afgrødekode 1) + Pligtige efterafgrøder (afgrødekode 968)
    T_eft = -30.0  # typetal for pligtige efterafgrøder
    H_test_3 = 100.0
    # For this case, we sum the N2O from Vårbyg and Efterafgrøder
    n2o_vaar_3, _ = calculate_n2o_nitratudvaskning(T_vaar, H_test_3)
    n2o_eft_3, _ = calculate_n2o_nitratudvaskning(T_eft, H_test_3)
    total_n2o_3 = n2o_vaar_3 + n2o_eft_3
    total_co2e_3 = total_n2o_3 * THETA_N2O_CO2
    print(f"\nTest Case 3 (Vårbyg + Efterafgrøder):")
    print(f"  Nitratudvaskning (Vårbyg): {n2o_vaar_3} kg N2O")
    print(f"  Nitratudvaskning (Efterafgrøder): {n2o_eft_3} kg N2O")
    print(f"  Total Nitratudvaskning: {total_n2o_3} kg N2O")
    print(f"  Omregnet til CO2e: {total_co2e_3} kg CO2e")
    # Expected N2O: 38.892857142857146 kg, Expected CO2e: 10306.607142857142 kg (using 265)
    # formulas.md output: 11590.07142857143, which is 38.892857142857146 * 298. Again, discrepancy.

    print("\nNote: Discrepancies in CO2e values compared to formulas.md exist.")
    print("This implementation uses theta_N2O_CO2 = 265.0 as defined in the notebook cell.")
    print("The formulas.md examples seem to use a factor of 298 for N2O to CO2e.")