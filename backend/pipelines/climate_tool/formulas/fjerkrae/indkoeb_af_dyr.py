def beregn_co2e_indkoeb_slagtekyllinger(a_daggamle: float, theta_daggamle: float, a_rugeaeg: float, theta_rugeaeg: float, a_slagt: float) -> float:
    """
    Beregner CO2e fra indkøb af daggamle slagtekyllinger og rugeæg.

    Args:
        a_daggamle: Antal indkøbte daggamle slagtekyllinger.
        theta_daggamle: Gns. klimaværdi for daggamle slagtekyllinger (kg CO2e/stk).
        a_rugeaeg: Antal indkøbte rugeæg.
        theta_rugeaeg: Gns. klimaværdi for rugeæg (kg CO2e/stk).
        a_slagt: Det totale antal producerede slagtekyllinger.

    Returns:
        CO2e fra indkøb (kg CO2e pr produceret slagtekylling).
    """
    if a_slagt == 0: # Prevent division by zero
        return 0.0
    co2e_indkoeb = (a_daggamle * theta_daggamle + a_rugeaeg * theta_rugeaeg) / a_slagt
    return co2e_indkoeb

def beregn_co2e_indkoeb_hoenniker(a_konsum: float, theta_konsum: float, a_leve: float, theta_leve: float, a_hoennike_prod: float) -> float:
    """
    Beregner CO2e fra indkøb af daggamle levekyllinger til hønnikeproduktion.

    Args:
        a_konsum: Antal indkøbte daggamle levekyllinger til konsumægsproduktion.
        theta_konsum: Gns. klimaværdi for daggamle levekyllinger til konsumægsproduktion (kg CO2e/stk).
        a_leve: Antal indkøbte daggamle levekyllinger til rugeægsproduktion.
        theta_leve: Gns. klimaværdi for daggamle levekyllinger til rugeægsproduktion (kg CO2e/stk).
        a_hoennike_prod: Det totale antal producerede hønniker.

    Returns:
        CO2e fra indkøb (kg CO2e pr produceret hønnike).
    """
    if a_hoennike_prod == 0: # Prevent division by zero
        return 0.0
    co2e_indkoeb = (a_konsum * theta_konsum + a_leve * theta_leve) / a_hoennike_prod
    return co2e_indkoeb

def beregn_co2e_indkoeb_hoener(a_hoenniker_indkoeb: float, theta_hoenniker: float, a_hoene_prod: float) -> float:
    """
    Beregner CO2e fra indkøb af hønniker til ægproduktion.

    Args:
        a_hoenniker_indkoeb: Antal indkøbte hønniker.
        theta_hoenniker: Gns. klimaværdi for hønniker (kg CO2e/stk).
        a_hoene_prod: Det totale antal producerede høner.

    Returns:
        CO2e fra indkøb (kg CO2e pr produceret høne).
    """
    if a_hoene_prod == 0: # Prevent division by zero
        return 0.0
    co2e_indkoeb = (a_hoenniker_indkoeb * theta_hoenniker) / a_hoene_prod
    return co2e_indkoeb