from typing import List, Dict

def beregn_co2e_el_bedrift(e_ind_kwh: float, e_egen_kwh: float, o_el_kg_co2e_pr_kwh: float) -> float:
    """
    Beregner CO2e fra elforbrug på bedriftsniveau.

    Args:
        e_ind_kwh: Indkøbt el (kWh).
        e_egen_kwh: Egenproduceret el (kWh).
        o_el_kg_co2e_pr_kwh: Omregningsfaktor for el (kg CO2e/kWh).

    Returns:
        CO2e fra el på bedriftsniveau (kg CO2e).
    """
    # OBS hvis E_egen > E_ind, sættes strømforbruget til 0
    forbrug_netto_kwh = max(0, e_ind_kwh - e_egen_kwh)
    co2e_el = forbrug_netto_kwh * o_el_kg_co2e_pr_kwh
    return co2e_el

def beregn_co2e_el_vanding_mark(f_v_kwh: float, sum_ha_v: float, ha_a: float, o_el_kg_co2e_pr_kwh: float) -> float:
    """
    Beregner CO2e fra el til vanding for en specifik mark.

    Args:
        f_v_kwh: Elforbrug total til vanding på bedriften (kWh).
        sum_ha_v: Sum af hektar på bedriften, der kan vandes (ha).
        ha_a: Antal hektar for den specifikke mark a (ha).
        o_el_kg_co2e_pr_kwh: Omregningsfaktor for el (kg CO2e/kWh).

    Returns:
        CO2e fra el til vanding for mark a (kg CO2e).
    """
    if sum_ha_v == 0: # Prevent division by zero
        return 0.0

    co2e_el_vanding_mark_a = (f_v_kwh / sum_ha_v) * ha_a * o_el_kg_co2e_pr_kwh
    return co2e_el_vanding_mark_a

def beregn_co2e_el_andet_mark(f_t_kwh: float, sum_ha_m: float, ha_a: float, o_el_kg_co2e_pr_kwh: float) -> float:
    """
    Beregner CO2e fra andet elforbrug (tørring mv.) for en specifik mark.

    Args:
        f_t_kwh: Elforbrug total til tørring mv. på bedriften (kWh).
        sum_ha_m: Sum af hektar på bedriften med afgrøder høstet til modenhed (ha).
        ha_a: Antal hektar for den specifikke mark a (ha).
        o_el_kg_co2e_pr_kwh: Omregningsfaktor for el (kg CO2e/kWh).

    Returns:
        CO2e fra andet elforbrug for mark a (kg CO2e).
    """
    if sum_ha_m == 0: # Prevent division by zero
        return 0.0

    co2e_el_andet_mark_a = (f_t_kwh / sum_ha_m) * ha_a * o_el_kg_co2e_pr_kwh
    return co2e_el_andet_mark_a