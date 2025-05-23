def beregn_bedriftsaftryk_kvaeg(farmahead_data: dict, mark_data: dict) -> float:
    """
    Beregner bedriftsaftrykket for kvæg ved at kombinere data fra ARLA's FarmAhead
    beregninger med data fra markmodulet i ESGreenTool.

    Fra FarmAhead tages data om udledning fra stald og lager, fordøjelse,
    importerede dyr og importeret foder.

    Fra ESGreenTool tages data om udledninger fra marken til produktionen af
    grovfoder samt eventuelle salgsafgrøder på bedriften. Aftrykket for alt
    egenproduceret foder antages at ligge i "mark" delen af bedriftsresultatet.

    Args:
        farmahead_data: Dictionary med data fra FarmAhead API indeholdende:
                       - stald_og_lager: CO2e fra stald og lager (kg CO2e)
                       - fordoejelse: CO2e fra fordøjelse (kg CO2e)
                       - importerede_dyr: CO2e fra importerede dyr (kg CO2e)
                       - importeret_foder: CO2e fra importeret foder (kg CO2e)
        mark_data: Dictionary med data fra ESGreenTool markmodul indeholdende:
                  - grovfoder: CO2e fra produktion af grovfoder (kg CO2e)
                  - salgsafgroeder: CO2e fra salgsafgrøder (kg CO2e)

    Returns:
        Samlet bedriftsaftryk for kvæg (kg CO2e).
    """
    # Summer FarmAhead komponenter
    farmahead_total = (
        farmahead_data.get('stald_og_lager', 0.0) +
        farmahead_data.get('fordoejelse', 0.0) +
        farmahead_data.get('importerede_dyr', 0.0) +
        farmahead_data.get('importeret_foder', 0.0)
    )

    # Summer ESGreenTool mark komponenter
    mark_total = (
        mark_data.get('grovfoder', 0.0) +
        mark_data.get('salgsafgroeder', 0.0)
    )

    # Samlet bedriftsaftryk
    bedriftsaftryk = farmahead_total + mark_total

    return bedriftsaftryk
