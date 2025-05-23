def beregn_co2e_hoens_til_slagtning(s_slagt: float, t_slagt: float, s_aeg: float, a_aeg: float, a_hoene: float, co2e_tot: float) -> float:
    """
    Beregner CO2e fra høner til slagtning.

    Args:
        s_slagt: Gns. salgspris pr kg høne (kr/kg).
        t_slagt: Den totale mængde høner leveret til slagtning (kg).
        s_aeg: Gns. salgspris pr. æg (kr/stk).
        a_aeg: Antal æg produceret pr holdhøne.
        a_hoene: Antal høner produceret på holdet i alt.
        co2e_tot: Mængden af CO2e udledt fra produktionen af en holdhøne inkl. æg (kg).

    Returns:
        CO2e fra høner til slagtning (kg CO2e).
    """
    # The formula in the notebook is: CO2e_slagt = (S_slagt * T_slagt) / (S_aeg * A_aeg * A_hoene) * CO2e_tot
    # However, the C# test case seems to imply a negative sign: CO2e_slagt = - (hons/aeg * CO2_aeg);
    # where hons = S_slagt * T_slagt and aeg = S_aeg * A_aeg (A_hoene is not used in aeg calculation in C#)
    # And CO2_aeg seems to be CO2e_tot in this context.
    # Given the purpose is to allocate some of the total CO2e to slaughter hens (reducing CO2e for eggs),
    # the negative sign makes sense if this value is later *subtracted* from the egg CO2e.
    # If this function's output is directly considered the CO2e *of* the slaughter hens, it should be positive.
    # The notebook title is "CO2e fra høner til slagtning", implying this is the emission attributed to them.
    # The C# test has `CO2e_slagt = - (hons/aeg * CO2_aeg);`
    # Then it calculates `CO2e_slagt/(A_aeg * (V_aeg/1000))` which is CO2e per kg egg, if CO2e_slagt was for eggs.
    # This is confusing. I will stick to the formula as written in the markdown: (S_slagt * T_slagt) / (S_aeg * A_aeg * A_hoene) * CO2e_tot
    # If A_hoene is 1 (for a single hen's lifecycle), it matches the C# test structure more closely if CO2e_tot is per hen.

    if s_aeg == 0 or a_aeg == 0 or a_hoene == 0: # Prevent division by zero
        return 0.0

    co2e_slagt = (s_slagt * t_slagt) / (s_aeg * a_aeg * a_hoene) * co2e_tot
    return co2e_slagt