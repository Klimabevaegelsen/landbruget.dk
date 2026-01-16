def beregn_co2e_varme_fjerkrae(v_o_liter: float, theta_o_kg_pr_l: float,
                               v_n_n3: float, theta_n_kg_pr_n3: float,
                               v_h_ton: float, theta_h_kg_pr_ton: float,
                               v_tp_ton: float, theta_tp_kg_pr_ton: float,
                               v_tf_ton: float, theta_tf_kg_pr_ton: float,
                               a_fjer: float) -> float:
    """
    Beregner CO2e fra varme til fjerkræ.

    Args:
        v_o_liter: Mængden af olie brugt til opvarmning (L).
        theta_o_kg_pr_l: Omregningsfaktor for olie (kg CO2e/L).
        v_n_n3: Mængden af naturgas brugt til opvarmning (N3 - normal m^3).
        theta_n_kg_pr_n3: Omregningsfaktor for naturgas (kg CO2e/N3).
        v_h_ton: Mængden af halm brugt til opvarmning (ton).
        theta_h_kg_pr_ton: Omregningsfaktor for halm (kg CO2e/ton).
        v_tp_ton: Mængden af træpiller brugt til opvarmning (ton).
        theta_tp_kg_pr_ton: Omregningsfaktor for træpiller (kg CO2e/ton).
        v_tf_ton: Mængden af træflis brugt til opvarmning (ton).
        theta_tf_kg_pr_ton: Omregningsfaktor for træflis (kg CO2e/ton).
        a_fjer: Det totale antal producerede fjerkræ som varmen skal fordeles på.

    Returns:
        CO2e fra varme (kg CO2e pr fjerkræ).
    """
    if a_fjer == 0: # Prevent division by zero
        return 0.0

    co2e_olie = v_o_liter * theta_o_kg_pr_l
    co2e_naturgas = v_n_n3 * theta_n_kg_pr_n3
    co2e_halm = v_h_ton * theta_h_kg_pr_ton
    co2e_traepiller = v_tp_ton * theta_tp_kg_pr_ton
    co2e_traeflis = v_tf_ton * theta_tf_kg_pr_ton

    total_co2e = co2e_olie + co2e_naturgas + co2e_halm + co2e_traepiller + co2e_traeflis

    co2e_varme_pr_fjer = total_co2e / a_fjer
    return co2e_varme_pr_fjer