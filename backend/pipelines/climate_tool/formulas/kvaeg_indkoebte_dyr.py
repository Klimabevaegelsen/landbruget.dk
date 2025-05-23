"""
Functions for calculating CO2e from purchased animals for cattle.
Based on the notebook: Kvaeg/Indkøbte dyr.ipynb
"""

def beregn_co2e_indkoebte_dyr_kvaeg(
    i_co2e: float,
    theta_maelk: float,
    fpcm: float,
    phi: float,
    n_ko: float
) -> float:
    """
    Beregn den samlede CO2e-udledning fra indkøbte dyr til kvæg.

    Args:
        i_co2e: Importerede dyr omregnet til CO2e (kg CO2e/kg FPCM).
                (Note: Kilden angiver dette som '???' fra ARLA API).
        theta_maelk: Allokeringsfaktor for mælk.
        fpcm: Mælkeproduktion pr. ko, leveret til mejeri (kg).
        phi: Antaget spildprocent på gården.
        n_ko: Antallet af køer.

    Returns:
        float: Den samlede CO2e-udledning fra indkøbte dyr i kg CO2e.
    """
    if theta_maelk == 0: # Prevent division by zero
        return 0.0

    co2e_indkoebte_dyr = (i_co2e / theta_maelk) * fpcm * phi * n_ko
    return co2e_indkoebte_dyr