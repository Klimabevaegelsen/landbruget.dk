def beregn_co2e_el_kvaeg(n_ko: float, e_ko: float, o_el: float) -> float:
    """
    Beregner CO2e fra el for kvæg.

    Args:
        n_ko: Antallet af køer.
        e_ko: Standard elforbrug pr. ko (kWh/ko).
        o_el: Omregningsfaktor for el (kg CO2/kWh).

    Returns:
        CO2e fra el (kg CO2e).
    """
    co2e_el = n_ko * e_ko * o_el
    return co2e_el