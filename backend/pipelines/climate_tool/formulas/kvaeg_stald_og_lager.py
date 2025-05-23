"""
Beregning af CO2e fra stald og lager
"""

# NB aftrykket fra strøelsen i stalden indgår i beregningen

# Formel:
# CO2_stald_lager = (S_CO2e / theta_maelk) * FPCM * phi * N_ko

# Hvor:
# S_CO2e: Udledning fra stald og lager omregnet til CO2e = 'Farm_KPIManureStorageCO2eq' fra ARLA API [kg CO2e/kg FPCM]
# theta_maelk: Allokeringsfaktor for mælk = 'Farm_KPIAllocKeyMilk' fra ARLA API
# FPCM: Mælkeproduktion pr ko, leveret til mejeri = 'Farm_KPIFatProteinCorrectedMilkPerCow' fra ARLA API [kg]
# phi: Antaget spildprocent på gården = Tabelværdi
# N_ko: Antallet af køer = 'Farm_KPICowsNHeads' fra ARLA API


def calculate_co2_stald_lager(
    s_co2e: float, theta_maelk: float, fpcm: float, phi: float, n_ko: int
) -> float:
    """
    Calculates CO2e from stald and lager.

    Args:
        s_co2e (float): Udledning fra stald og lager omregnet til CO2e [kg CO2e/kg FPCM]
        theta_maelk (float): Allokeringsfaktor for mælk
        fpcm (float): Mælkeproduktion pr ko, leveret til mejeri [kg]
        phi (float): Antaget spildprocent på gården
        n_ko (int): Antallet af køer

    Returns:
        float: CO2_stald_lager in kg CO2e
    """
    co2_stald_lager = (s_co2e / theta_maelk) * fpcm * phi * n_ko
    return co2_stald_lager


# Testcases
if __name__ == "__main__":
    # Test case from Kvaeg/Stald_og_lager.ipynb
    S_CO2e_test = 0.05
    theta_maelk_test = 0.87
    FPCM_test = 10456.95
    phi_test = 1.05
    N_ko_test = 109

    result = calculate_co2_stald_lager(
        S_CO2e_test, theta_maelk_test, FPCM_test, phi_test, N_ko_test
    )
    print(f"Stald og lager: {result} kg ({result / 1000} ton) CO2e")
    # Expected: Stald og lager: 68781.4900862069 kg (68.7814900862069 ton) CO2e

    # Additional test cases can be added here
    # Example with different values:
    S_CO2e_test_2 = 0.06
    theta_maelk_test_2 = 0.90
    FPCM_test_2 = 11000
    phi_test_2 = 1.02
    N_ko_test_2 = 150
    result_2 = calculate_co2_stald_lager(
        S_CO2e_test_2, theta_maelk_test_2, FPCM_test_2, phi_test_2, N_ko_test_2
    )
    print(f"Stald og lager (Test 2): {result_2} kg ({result_2 / 1000} ton) CO2e")