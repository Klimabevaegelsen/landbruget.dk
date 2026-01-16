from typing import List, Dict

def beregn_produktaftryk_slagtekyllinger_pr_kg(co2e_total_pr_kylling: float, v_slagt_gram: float) -> float:
    """
    Beregner produktaftrykket for slagtekyllinger pr. kg.

    Args:
        co2e_total_pr_kylling: Den totale mængde CO2e fra produktion af én slagtekylling (kg CO2e).
        v_slagt_gram: Gennemsnitlig levende vægt for en slagtekylling på holdet (gram).

    Returns:
        Produktaftryk pr kg slagtekylling (kg CO2e/kg levende vægt).
    """
    if v_slagt_gram == 0:  # Prevent division by zero
        return 0.0

    # Omregn fra gram til kg
    v_slagt_kg = v_slagt_gram / 1000.0

    # P_h = CO2e_tot / (V_slagt * 1000) - men V_slagt er allerede i gram, så vi deler med 1000 for at få kg
    p_h = co2e_total_pr_kylling / v_slagt_kg

    return p_h

def beregn_co2e_total_slagtekylling(
    co2e_el: float,
    co2e_enterisk_metan: float,
    co2e_foder: float,
    co2e_indkoeb_dyr: float,
    co2e_lager: float,
    co2e_stald: float,
    co2e_stroelse: float,
    co2e_varme: float
) -> float:
    """
    Beregner den totale CO2e udledning fra alle emissionskilder for en slagtekylling.

    Args:
        co2e_el: CO2e fra el (kg CO2e).
        co2e_enterisk_metan: CO2e fra enterisk metan (kg CO2e).
        co2e_foder: CO2e fra foder (kg CO2e).
        co2e_indkoeb_dyr: CO2e fra indkøb af rugeæg og daggamle slagtekyllinger (kg CO2e).
        co2e_lager: CO2e fra lager (kg CO2e).
        co2e_stald: CO2e fra stald (kg CO2e).
        co2e_stroelse: CO2e fra strøelse (kg CO2e).
        co2e_varme: CO2e fra varme (kg CO2e).

    Returns:
        Total CO2e udledning (kg CO2e).
    """
    co2e_total = (
        co2e_el +
        co2e_enterisk_metan +
        co2e_foder +
        co2e_indkoeb_dyr +
        co2e_lager +
        co2e_stald +
        co2e_stroelse +
        co2e_varme
    )

    return co2e_total

def beregn_bedriftsaftryk_slagtekyllinger(hold_data: List[Dict[str, float]]) -> float:
    """
    Beregner bedriftsaftrykket for slagtekyllinger pr. kalenderår.

    Args:
        hold_data: En liste af dictionaries, hvor hver dictionary repræsenterer et hold i et stald
                   og indeholder følgende nøgler:
                   'p_h': Produktresultatet for holdet (kg CO2e pr kg levende vægt).
                   'co2e_hjemme': CO2e aftrykket fra de hjemmeavlede råvarer for holdet (kg CO2e pr kylling).
                   'a_slagt': Antallet af slagtekyllinger på holdet (stk).
                   'v_slagt_gram': Gennemsnitlig levende vægt pr slagtekylling (gram).

    Returns:
        Bedriftsaftrykket for slagtekyllinger (kg CO2e).
    """
    # Formula: B = sum_over_stalls_sum_over_holds ( (P_h_j,i - CO2e_hjemme_j,i) * A_slagt_j,i )
    # P_h in this context is produktaftrykket pr kg, so we need to multiply by weight to get per bird

    bedriftsaftryk = 0.0
    for hold in hold_data:
        v_slagt_kg = hold['v_slagt_gram'] / 1000.0  # Convert from gram to kg
        produktaftryk_pr_kylling = hold['p_h'] * v_slagt_kg  # Convert from pr kg to pr bird
        bidrag_til_bedrift = (produktaftryk_pr_kylling - hold['co2e_hjemme']) * hold['a_slagt']
        bedriftsaftryk += bidrag_til_bedrift

    return bedriftsaftryk
