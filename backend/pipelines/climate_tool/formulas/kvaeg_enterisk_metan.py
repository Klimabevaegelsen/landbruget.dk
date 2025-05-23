"""
Beregn CO2e fra enterisk metan for kvæg.
Based on the note with calculation basis, pages 15-18.
"""

THETA_CH4_CO2 = 25.0  # GWP for CH4 from Markdown page 4

# Standardværdier for enterisk metan for kalve og tyre 0-6 mdr (kg CH4/periode)
# From Markdown page 17, Table "Standardværdi, kg CH4/periode"
ENTERIC_CH4_YOUNGSTOCK_DEFAULTS_KG_CH4_PER_PERIOD = {
    "opdraet_0_6mdr_tung": 8.48,
    "opdraet_0_6mdr_jersey": 4.65,
    "tyre_0_6mdr_tung": 13.22,
    "tyre_0_6mdr_jersey": 8.67
}

def calculate_ch4_enteric_malkeko_tung_race_pr_aar(
    foderoptag_kg_ts_pr_dag: float,
    fedtsyre_g_pr_kg_ts: float,
    ndf_g_pr_kg_ts: float
) -> float:
    """
    Beregner kg CH4 pr. årsko for malkekøer - tung race.
    Formula from Markdown page 17.
    kg CH4 pr. årsko = ((1.230 * Foderoptag_kg_ts - 0.145 * fedtsyre_g/kg_ts + 0.012 * NDF_g/kg_ts) / 55.65 * 335) + (0.304 * 30)
    """
    ch4_malkende_periode = (1.230 * foderoptag_kg_ts_pr_dag - \
                            0.145 * fedtsyre_g_pr_kg_ts + \
                            0.012 * ndf_g_pr_kg_ts) / 55.65 * 335.0
    ch4_goldperiode = 0.304 * 30.0
    return ch4_malkende_periode + ch4_goldperiode

def calculate_ch4_enteric_malkeko_jersey_pr_aar(
    foderoptag_kg_ts_pr_dag: float,
    fedtsyre_g_pr_kg_ts: float,
    ndf_g_pr_kg_ts: float
) -> float:
    """
    Beregner kg CH4 pr. årsko for malkekøer - Jersey.
    Formula from Markdown page 17.
    kg CH4 pr. årsko = ((1.230 * Foderoptag_kg_ts/ko/dag - 0.145 * fedtsyre_g/kg_ts/ko/dag + 0.012 * NDF_g/kg_ts/ko/dag) / 55.65 * 335) + (0.207 * 30)
    """
    ch4_malkende_periode = (1.230 * foderoptag_kg_ts_pr_dag - \
                            0.145 * fedtsyre_g_pr_kg_ts + \
                            0.012 * ndf_g_pr_kg_ts) / 55.65 * 335.0
    ch4_goldperiode = 0.207 * 30.0
    return ch4_malkende_periode + ch4_goldperiode

def calculate_ch4_enteric_opdraet_og_tyre_aeldre_pr_aar(
    foderoptag_kg_ts_pr_dag: float,
    kraftfoderandel_procent: float, # % af foderoptag
    fedtsyre_g_pr_kg_ts: float
) -> float:
    """
    Beregner kg CH4 pr. årsdyr for opdræt (6 mdr. til kælvning) og tyre (6 mdr. til slagtning).
    Formula from Markdown page 17.
    kg CH4 pr. årsdyr = (1.6978 + 0.5950 * Kraftfoderoptag + 1.4655 * grovfoderoptag - 0.00388 * fedtsyreindtag - 0.00308 * askeindtag) / 55.65 * 365
    Askeindtag fixed at 860 g/dyr/dag = 0.860 kg/dyr/dag.
    """
    kraftfoderoptag_kg_ts_pr_dag = foderoptag_kg_ts_pr_dag * (kraftfoderandel_procent / 100.0)
    grovfoderoptag_kg_ts_pr_dag = foderoptag_kg_ts_pr_dag - kraftfoderoptag_kg_ts_pr_dag
    fedtsyreindtag_g_pr_dag = foderoptag_kg_ts_pr_dag * fedtsyre_g_pr_kg_ts
    askeindtag_g_pr_dag = 860.0 # g/dyr/dag as per Markdown page 18

    ch4_numerator = (1.6978 + \
                     0.5950 * kraftfoderoptag_kg_ts_pr_dag + \
                     1.4655 * grovfoderoptag_kg_ts_pr_dag - \
                     0.00388 * fedtsyreindtag_g_pr_dag - \
                     0.00308 * askeindtag_g_pr_dag)

    ch4_kg_pr_aarsdyr = (ch4_numerator / 55.65) * 365.0
    return ch4_kg_pr_aarsdyr

def beregn_co2e_enterisk_kvaeg_total(
    dyretype_counts: dict # e.g. {"malkeko_tung_race": {"count": 50, "foderoptag_kg_ts_pr_dag": 22.0, ...}, ...}
                                 # For "opdraet_0_6mdr_tung", etc., only "count" is needed if using defaults.
) -> float:
    """
    Beregner den totale CO2e udledning fra enterisk metan for alt kvæg på bedriften.
    """
    total_co2e = 0.0

    for dyretype, data in dyretype_counts.items():
        count = data.get("count", 0)
        if count == 0:
            continue

        ch4_pr_dyr_pr_aar = 0.0
        if dyretype == "malkeko_tung_race":
            ch4_pr_dyr_pr_aar = calculate_ch4_enteric_malkeko_tung_race_pr_aar(
                foderoptag_kg_ts_pr_dag=data["foderoptag_kg_ts_pr_dag"],
                fedtsyre_g_pr_kg_ts=data["fedtsyre_g_pr_kg_ts"],
                ndf_g_pr_kg_ts=data["ndf_g_pr_kg_ts"]
            )
        elif dyretype == "malkeko_jersey":
            ch4_pr_dyr_pr_aar = calculate_ch4_enteric_malkeko_jersey_pr_aar(
                foderoptag_kg_ts_pr_dag=data["foderoptag_kg_ts_pr_dag"],
                fedtsyre_g_pr_kg_ts=data["fedtsyre_g_pr_kg_ts"],
                ndf_g_pr_kg_ts=data["ndf_g_pr_kg_ts"]
            )
        elif dyretype in ["opdraet_aeldre_tung", "opdraet_aeldre_jersey", "tyre_aeldre_tung", "tyre_aeldre_jersey"]:
            # Assuming a common function for older heifers and bulls based on structure
            ch4_pr_dyr_pr_aar = calculate_ch4_enteric_opdraet_og_tyre_aeldre_pr_aar(
                foderoptag_kg_ts_pr_dag=data["foderoptag_kg_ts_pr_dag"],
                kraftfoderandel_procent=data["kraftfoderandel_procent"],
                fedtsyre_g_pr_kg_ts=data["fedtsyre_g_pr_kg_ts"]
            )
        elif dyretype in ENTERIC_CH4_YOUNGSTOCK_DEFAULTS_KG_CH4_PER_PERIOD:
            # These are per period, not per year directly. The problem asks for annual totals.
            # Assuming these defaults represent the total CH4 for the 0-6 month period for one animal.
            # If an animal is present for a full year and goes through this phase, this is its CH4 for that phase.
            # The structure might need adjustment if these are rates or for animals not completing the period.
            # For simplicity, if an animal of this type is counted, we add its period CH4.
            # This implies the 'count' is of animals completing this 0-6 month phase in the year.
            ch4_pr_dyr_pr_aar = ENTERIC_CH4_YOUNGSTOCK_DEFAULTS_KG_CH4_PER_PERIOD[dyretype]
        else:
            print(f"Warning: Unknown dyretype for enteric methane: {dyretype}")
            continue

        total_co2e += ch4_pr_dyr_pr_aar * count * THETA_CH4_CO2

    return total_co2e


if __name__ == "__main__":
    print("Eksempel: Malkeko Tung Race")
    ch4_mktr = calculate_ch4_enteric_malkeko_tung_race_pr_aar(
        foderoptag_kg_ts_pr_dag=24.0, # Example value
        fedtsyre_g_pr_kg_ts=20.0,    # Example value
        ndf_g_pr_kg_ts=350.0         # Example value
    )
    print(f"  CH4 pr. årsko (tung race): {ch4_mktr:.2f} kg")
    print(f"  CO2e pr. årsko (tung race): {ch4_mktr * THETA_CH4_CO2:.2f} kg")

    print("\nEksempel: Opdræt Ældre")
    ch4_opdr_aeldre = calculate_ch4_enteric_opdraet_og_tyre_aeldre_pr_aar(
        foderoptag_kg_ts_pr_dag=7.7, # from Markdown default opdræt 6 mdr. tung race
        kraftfoderandel_procent=9.0, # from Markdown default
        fedtsyre_g_pr_kg_ts=19.0     # from Markdown default
    )
    print(f"  CH4 pr. årsdyr (opdræt ældre): {ch4_opdr_aeldre:.2f} kg")
    print(f"  CO2e pr. årsdyr (opdræt ældre): {ch4_opdr_aeldre * THETA_CH4_CO2:.2f} kg")

    print("\nEksempel: Opdræt 0-6 mdr Tung Race (default value)")
    ch4_opdr_0_6_tung = ENTERIC_CH4_YOUNGSTOCK_DEFAULTS_KG_CH4_PER_PERIOD["opdraet_0_6mdr_tung"]
    print(f"  CH4 pr. dyr pr. periode (opdræt 0-6mdr tung): {ch4_opdr_0_6_tung:.2f} kg")
    print(f"  CO2e pr. dyr pr. periode (opdræt 0-6mdr tung): {ch4_opdr_0_6_tung * THETA_CH4_CO2:.2f} kg")

    print("\nEksempel: Samlet beregning for en bedrift")
    test_bedrift_dyr = {
        "malkeko_tung_race": {
            "count": 100,
            "foderoptag_kg_ts_pr_dag": 23.5,
            "fedtsyre_g_pr_kg_ts": 22.0,
            "ndf_g_pr_kg_ts": 340.0
        },
        "malkeko_jersey": {
            "count": 50,
            "foderoptag_kg_ts_pr_dag": 19.0,
            "fedtsyre_g_pr_kg_ts": 28.0,
            "ndf_g_pr_kg_ts": 330.0
        },
        "opdraet_aeldre_tung": { # > 6 months
            "count": 40,
            "foderoptag_kg_ts_pr_dag": 7.5,
            "kraftfoderandel_procent": 10.0,
            "fedtsyre_g_pr_kg_ts": 18.0
        },
        "opdraet_0_6mdr_tung": {"count": 20},
        "tyre_0_6mdr_jersey": {"count": 15}
    }
    total_co2e_bedrift = beregn_co2e_enterisk_kvaeg_total(test_bedrift_dyr)
    print(f"  Total CO2e fra enterisk metan for testbedrift: {total_co2e_bedrift:.2f} kg CO2e")