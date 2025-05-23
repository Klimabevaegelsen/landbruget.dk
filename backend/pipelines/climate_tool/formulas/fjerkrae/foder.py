def beregn_co2e_slagtekyllingefoder(t_total: float, a_fuld: float, a_til: float, a_hvede: float, i_hvede: float, co2e_fuld: float, co2e_til: float, co2e_egen: float, co2e_ind: float, a_grov: float = 0.0, co2e_grov: float = 0.0) -> float:
    """
    Beregner CO2e fra foder til slagtekyllinger.

    Args:
        t_total: Den totale mængde foder pr indsat slagtekylling (kg).
        a_fuld: Andelen af fuldfoder (%).
        a_til: Andelen af tilskudsfoder (%).
        a_hvede: Andelen af hel hvede (%).
        i_hvede: Andelen af indkøbt hel hvede (%).
        co2e_fuld: Mængden af CO2e i fuldfoder (kg CO2e/kg).
        co2e_til: Mængden af CO2e i tilskudsfoder (kg CO2e/kg).
        co2e_egen: Mængden af CO2e i egenproduceret hel hvede (kg CO2e/kg).
        co2e_ind: Mængden af CO2e i indkøbt hel hvede (kg CO2e/kg).
        a_grov: Andelen af grovfoder (%). Defaults to 0.0.
        co2e_grov: Mængden af CO2e i grovfoder (kg CO2e/kg). Defaults to 0.0.

    Returns:
        CO2e fra slagtekyllingefoder (kg CO2e).
    """
    f_fuld = t_total * a_fuld * co2e_fuld
    f_til = t_total * a_til * co2e_til
    f_hvede = t_total * a_hvede * (1 - i_hvede) * co2e_egen + t_total * a_hvede * i_hvede * co2e_ind
    f_grov = t_total * a_grov * co2e_grov

    co2e_slagtekyllingefoder = f_fuld + f_til + f_hvede + f_grov
    return co2e_slagtekyllingefoder

def beregn_co2e_aeglaeggerfoder(m_fuld: float, m_til: float, m_skal: float, m_korn: float, m_grov: float, i_korn: float, co2e_fuld: float, co2e_til: float, co2e_skal: float, co2e_egen_korn: float, co2e_ind_korn: float, co2e_grov: float, a_hoene: float) -> float:
    """
    Beregner CO2e fra foder til æglæggende høner.

    Args:
        m_fuld: Mængden af fuldfoder (kg).
        m_til: Mængden af tilskudsfoder (kg).
        m_skal: Mængden af æggeskaller (kg).
        m_korn: Mængden af korn (kg).
        m_grov: Mængden af grovfoder (kg).
        i_korn: Mængden af indkøbt korn (kg).
        co2e_fuld: Mængden af CO2e i fuldfoder (kg CO2e/kg).
        co2e_til: Mængden af CO2e i tilskudsfoder (kg CO2e/kg).
        co2e_skal: Mængden af CO2e i æggeskaller (kg CO2e/kg).
        co2e_egen_korn: Mængden af CO2e i egenproduceret korn (kg CO2e/kg).
        co2e_ind_korn: Mængden af CO2e i indkøbte råvarer (kg CO2e/kg).
        co2e_grov: Mængden af CO2e i grovfoder (kg CO2e/kg).
        a_hoene: Antal høner produceret på holdet i alt.

    Returns:
        CO2e fra æglæggerfoder (kg CO2e).
    """
    f_fuld = m_fuld * co2e_fuld
    f_til = m_til * co2e_til
    f_skal = m_skal * co2e_skal
    f_korn = (m_korn - i_korn) * co2e_egen_korn + i_korn * co2e_ind_korn
    f_grov = m_grov * co2e_grov

    co2e_aeglaeggerfoder = (f_fuld + f_til + f_skal + f_korn + f_grov) / a_hoene
    return co2e_aeglaeggerfoder