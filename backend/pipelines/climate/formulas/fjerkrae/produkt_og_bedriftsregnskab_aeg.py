from typing import List, Dict

def beregn_produktaftryk_aeg_pr_kg(co2e_total_pr_holdhoene: float, co2e_slagt_pr_holdhoene: float, v_aeg_gram: float, a_aeg_pr_holdhoene: float) -> float:
    """
    Beregner produktaftrykket for æg pr. kg.

    Args:
        co2e_total_pr_holdhoene: Den totale mængde CO2e fra én holdhøne inkl. æg (kg CO2e).
        co2e_slagt_pr_holdhoene: CO2e udledningen fra en slagtehøne, allokeret til den ene holdhøne (kg CO2e).
        v_aeg_gram: Gns. vægt for et æg (gram).
        a_aeg_pr_holdhoene: Antal æg produceret per holdhøne i hele holdets løbetid (stk).

    Returns:
        Produktaftryk pr kg æg (kg CO2e/kg æg).
    """
    # P_h = CO2e_tot - CO2e_slagt
    p_h = co2e_total_pr_holdhoene - co2e_slagt_pr_holdhoene

    # Total vægt af æg pr holdhøne i kg
    total_aeg_kg_pr_holdhoene = (v_aeg_gram / 1000.0) * a_aeg_pr_holdhoene

    if total_aeg_kg_pr_holdhoene == 0:  # Prevent division by zero
        return 0.0

    # P_æg = P_h / ((V_æg / 1000) * A_æg)
    p_aeg_pr_kg = p_h / total_aeg_kg_pr_holdhoene
    return p_aeg_pr_kg

def beregn_co2e_total_holdhoene(
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
    Beregner den totale CO2e udledning fra alle emissionskilder for en holdhøne.

    Args:
        co2e_el: CO2e fra el (kg CO2e).
        co2e_enterisk_metan: CO2e fra enterisk metan (kg CO2e).
        co2e_foder: CO2e fra foder (kg CO2e).
        co2e_indkoeb_dyr: CO2e fra indkøb af hønniker (kg CO2e).
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

def beregn_bedriftsaftryk_aeg(hold_data: List[Dict[str, float]]) -> float:
    """
    Beregner bedriftsaftrykket for æg pr. kalenderår.

    Args:
        hold_data: En liste af dictionaries, hvor hver dictionary repræsenterer et hold i et stald
                   og indeholder følgende nøgler:
                   'p_h': Produktresultatet for holdet (kg CO2e pr holdhøne før slagtallokering).
                   'co2e_hjemme': CO2e aftrykket fra de hjemmeavlede råvarer for holdet (kg CO2e pr holdhøne).
                   'a_aars': Antallet af årshøner på holdet.
                   'co2e_slagt': CO2e allokeret til slagt fra dette hold (kg CO2e pr holdhøne).

    Returns:
        Bedriftsaftrykket for æg (kg CO2e).
    """
    # Formula: B = sum_over_stalls_sum_over_holds ( (P_h_j,i - CO2e_hjemme_j,i) * A_års_j,i )
    # P_h in the formula is CO2e_tot (total per hen) - CO2e_slagt (allocated to slaughter)
    # The input `p_h` to this function from `hold_data` is expected to be `CO2e_total_pr_holdhoene` (the sum of emission sources for the hen)
    # and `co2e_slagt` is the part allocated to slaughter for that hen.

    bedriftsaftryk = 0.0
    for hold in hold_data:
        p_h_netto = hold['p_h'] - hold['co2e_slagt']  # This is P_h from the formula: CO2e_tot - CO2e_slagt
        bidrag_til_bedrift = (p_h_netto - hold['co2e_hjemme']) * hold['a_aars']
        bedriftsaftryk += bidrag_til_bedrift

    return bedriftsaftryk
