def beregn_co2e_enterisk_kvaeg(e_co2e: float, theta_maelk: float, fpcm: float, phi: float, n_ko: float) -> float:
    """
    Beregner CO2e fra enterisk metan for kvæg.

    Args:
        e_co2e: Enterisk metan fra fordøjelsen omregnet til CO2e (kg CO2e/kg FPCM).
        theta_maelk: Allokeringsfaktor for mælk.
        fpcm: Mælkeproduktion pr. ko, leveret til mejeri (kg).
        phi: Antaget spildprocent på gården.
        n_ko: Antallet af køer.

    Returns:
        CO2e fra enterisk metan (kg CO2e).
    """
    co2_enterisk = (e_co2e / theta_maelk) * fpcm * phi * n_ko
    return co2_enterisk