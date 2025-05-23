def beregn_co2e_el_fjerkrae(e_total: float, e_egen: float, o_el: float, a_fjer: float) -> float:
    """
    Beregner CO2e fra el til fjerkræ.

    Args:
        e_total: Mængden af el til holdet (kWh).
        e_egen: Mængden af egenproduceret strøm (kWh).
        o_el: Omregningsfaktor for el (kg CO2/kWh).
        a_fjer: Det totale antal producerede fjerkræ som elmængden skal fordeles på.

    Returns:
        CO2e fra el (kg CO2e).
    """
    co2e_el = ((e_total - e_egen) * o_el) / a_fjer
    return co2e_el