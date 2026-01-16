def beregn_co2e_hjemmeavlet_slagtekyllinger(t_total: float, a_x: float, i_x: float, co2e_x: float) -> float:
    """
    Beregner CO2e fra hjemmeavlet hel hvede/råvarer til slagtekyllinger.

    Args:
        t_total: Den totale mængde foder pr dyr (kg).
        a_x: Andelen af hel hvede/råvarer i foderet (%).
        i_x: Andelen af indkøbt hel hvede/råvarer i foderet (resterende antages som værende hjemmeavlet) (%).
        co2e_x: Mængden af CO2e i hjemmeavlet hel hvede/råvarer m/u dLUC (kg CO2e/kg).

    Returns:
        CO2e fra hjemmeavlet foder (kg CO2e).
    """
    # Assuming i_x is a percentage, convert to decimal for calculation (100% - I_x)
    # The formula seems to use (100% - I_x) which implies I_x is a percentage of purchased feed.
    # If I_x is the proportion of purchased, then (1 - I_x) is the proportion of home-grown.
    # However, the description says "Andelen af indkøbt hel hvede/råvarer i foderet (resterende antages som værende hjemmeavlet)"
    # This suggests i_x is the proportion *of that specific component* that is purchased.
    # The formula given is T_total * A_X * (100% - I_X) * CO2e_X. If I_X is % purchased, then (1-I_X) is % homegrown.
    co2e_hjemme = t_total * a_x * (1.0 - i_x) * co2e_x
    return co2e_hjemme

def beregn_co2e_hjemmeavlet_hoener(m_korn: float, i_korn: float, co2e_korn: float, a_hoene: float) -> float:
    """
    Beregner CO2e fra hjemmeavlet korn til høner.

    Args:
        m_korn: Mængden af korn (kg).
        i_korn: Mængden af indkøbt korn (kg).
        co2e_korn: Mængden af CO2e i hjemmeavlet korn m/u dLUC (kg CO2e/kg).
        a_hoene: Antal høner produceret på holdet i alt.

    Returns:
        CO2e fra hjemmeavlet foder (kg CO2e).
    """
    co2e_hjemme = ((m_korn - i_korn) * co2e_korn) / a_hoene
    return co2e_hjemme