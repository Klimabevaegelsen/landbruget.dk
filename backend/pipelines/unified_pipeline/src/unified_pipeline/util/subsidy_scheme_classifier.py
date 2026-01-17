"""
Subsidy Scheme Classifier

Standardizes Danish agricultural subsidy scheme names to consistent codes.

This module handles:
1. EU CAP (Common Agricultural Policy) schemes from støtteoplysninger
2. National schemes (de minimis, slaughter premium)
3. FVM commitment types (organic, grassland, environmental)

Scheme Code Format:
- GRUNDBETALING: Basic payment scheme
- GROEN_STOETTE: Greening payment
- OEKOLOGISK: Organic farming
- GRAESPLEJE: Grassland management
- MILJOE: Environmental schemes
- NATURA2000: Natura 2000 payments
- SLAGTEPRAEMIE: Slaughter premium (voluntary coupled support)
- DEMINIMIS: De minimis aid
"""

# EU CAP Scheme mapping from støtteoplysninger "Scheme" column
# Maps English scheme names to standardized Danish codes
EU_SCHEME_MAPPING = {
    # Pillar 1 - Direct Payments (EAGF)
    "Basic payment scheme": ("GRUNDBETALING", "Grundbetaling"),
    "Payment for agricultural practices beneficial for the climate and the environment": (
        "GROEN_STOETTE",
        "Grøn støtte",
    ),
    "Payment for agricultural practices beneficial": ("GROEN_STOETTE", "Grøn støtte"),
    "Voluntary coupled support": ("KOBLET_STOETTE", "Koblet støtte"),
    "Redistributive payment": ("OMFORDELING", "Omfordelingsstøtte"),
    "Payment for young farmers": ("UNGE_LANDMAEND", "Unge landmænd"),
    "Small farmers scheme": ("SMAA_LANDMAEND", "Små landmænd"),
    # Pillar 2 - Rural Development (EAFRD)
    "Organic farming": ("OEKOLOGISK", "Økologisk arealtilskud"),
    "Agri-environment-climate": ("GRAESPLEJE", "Pleje af græs- og naturarealer"),
    "Agri-environment-climate - commitments": ("GRAESPLEJE", "Pleje af græs- og naturarealer"),
    "Natura 2000 and water framework directive payments": ("NATURA2000", "Natura 2000"),
    "Natura 2000": ("NATURA2000", "Natura 2000"),
    "Areas facing natural constraints": ("NATURLIGE_BEGR", "Arealer med naturlige begrænsninger"),
    "Farm and business development": ("LANDBRUG_UDVIKLING", "Landbrugs- og forretningsudvikling"),
    "Animal welfare payments": ("DYREVELFAERD", "Dyrevelfærd"),
    # Summary row (must be filtered!)
    "Total for beneficiary": ("TOTAL_SUMMARY", "Total for modtager"),
}

# FVM Tilsagnstype codes (commitment types)
# From Tilsagn_til_økologiske_arealtilskud, Tilsagn_til_pleje_af_græs, etc.
FVM_TILSAGN_MAPPING = {
    # Organic (code 36, 37, etc.)
    "36": ("OEKOLOGISK_36", "Økologisk arealtilskud - Omlægning"),
    "37": ("OEKOLOGISK_37", "Økologisk arealtilskud - Opretholdelse"),
    "36B": ("OEKOLOGISK_36B", "Økologisk arealtilskud - Basistilskud"),
    "37B": ("OEKOLOGISK_37B", "Økologisk arealtilskud - Omlægning basis"),
    # Grassland (code 66, 67, etc.)
    "66": ("GRAESPLEJE_66", "Pleje af græs - Uden afgræsning"),
    "67": ("GRAESPLEJE_67", "Pleje af græs - Med afgræsning"),
    "67B": ("GRAESPLEJE_67B", "Pleje af græs - Basis afgræsning"),
    "68": ("GRAESPLEJE_68", "Pleje af græs - Høslet"),
    # Environmental
    "70": ("MILJOE_70", "Miljøvenlig drift"),
    "71": ("MILJOE_71", "Biodiversitet og bæredygtighed"),
}

# De minimis scheme codes
DEMINIMIS_MAPPING = {
    "MRDM": ("DEMINIMIS_MRDM", "De minimis - Landbrugsrelateret"),
    "MREG": ("DEMINIMIS_MREG", "De minimis - Regional"),
    "MIKO": ("DEMINIMIS_MIKO", "De minimis - Økologisk"),
    "LANDX": ("DEMINIMIS_LANDX", "De minimis - Landbrugsstøtte"),
}

# Data-derived payment rates (kr/ha)
# These are MORE accurate than official rates (10-20% better match to actual payments)
# Based on analysis in .context/subsidies_granularity_investigation.md
DERIVED_PAYMENT_RATES = {
    # Organic (derived from 2022-2023 data)
    "OEKOLOGISK_36": 868,  # Official: 955 kr/ha
    "OEKOLOGISK_37": 1367,  # Official: 1,671 kr/ha
    "OEKOLOGISK_36B": 870,
    "OEKOLOGISK_37B": 1350,
    # Grassland (derived from 2022-2023 data)
    "GRAESPLEJE_66": 2600,  # Official: ~2,600 kr/ha (matches)
    "GRAESPLEJE_67": 1650,  # Official: ~1,800 kr/ha
    "GRAESPLEJE_67B": 1600,
    "GRAESPLEJE_68": 2100,
    # Environmental
    "MILJOE_70": 1200,
    "MILJOE_71": 1500,
    # Basic payment (varies by farm - payment rights)
    # These are average rates; actual varies by farm's historical payment rights
    "GRUNDBETALING": 1995,  # Average, std dev ~282 kr/ha
    "GROEN_STOETTE": 500,  # Greening component
}

# Official rates for reference (not used in calculations)
OFFICIAL_PAYMENT_RATES = {
    "OEKOLOGISK_36": 955,
    "OEKOLOGISK_37": 1671,
    "GRAESPLEJE_66": 2600,
    "GRAESPLEJE_67": 1800,
}


def classify_eu_scheme(scheme_name: str) -> tuple[str | None, str | None]:
    """
    Classify an EU scheme name to standardized code.

    Args:
        scheme_name: Scheme name from støtteoplysninger "Scheme" column

    Returns:
        Tuple of (ordning_kode, ordning_dansk) or (None, None) if not found

    Examples:
        >>> classify_eu_scheme("Basic payment scheme")
        ('GRUNDBETALING', 'Grundbetaling')
        >>> classify_eu_scheme("Total for beneficiary")
        ('TOTAL_SUMMARY', 'Total for modtager')
    """
    if not scheme_name:
        return None, None

    # Exact match
    if scheme_name in EU_SCHEME_MAPPING:
        return EU_SCHEME_MAPPING[scheme_name]

    # Partial match (handle truncated names)
    scheme_lower = scheme_name.lower()
    for key, value in EU_SCHEME_MAPPING.items():
        if key.lower() in scheme_lower or scheme_lower in key.lower():
            return value

    return None, None


def classify_fvm_tilsagn(tilsagnstype: str) -> tuple[str | None, str | None]:
    """
    Classify an FVM commitment type code.

    Args:
        tilsagnstype: Tilsagnstype code from FVM data (e.g., "36", "67")

    Returns:
        Tuple of (ordning_kode, ordning_dansk) or (None, None) if not found

    Examples:
        >>> classify_fvm_tilsagn("36")
        ('OEKOLOGISK_36', 'Økologisk arealtilskud - Omlægning')
        >>> classify_fvm_tilsagn("67")
        ('GRAESPLEJE_67', 'Pleje af græs - Med afgræsning')
    """
    if not tilsagnstype:
        return None, None

    code = str(tilsagnstype).strip().upper()
    if code in FVM_TILSAGN_MAPPING:
        return FVM_TILSAGN_MAPPING[code]

    return None, None


def classify_deminimis(ordning: str) -> tuple[str | None, str | None]:
    """
    Classify a de minimis scheme code.

    Args:
        ordning: Ordning code from de minimis data

    Returns:
        Tuple of (ordning_kode, ordning_dansk) or (None, None) if not found
    """
    if not ordning:
        return None, None

    code = str(ordning).strip().upper()
    if code in DEMINIMIS_MAPPING:
        return DEMINIMIS_MAPPING[code]

    return None, None


def get_expected_rate(ordning_kode: str, use_derived: bool = True) -> float | None:
    """
    Get the expected payment rate for a scheme.

    Args:
        ordning_kode: Standardized scheme code
        use_derived: If True, use data-derived rates (more accurate).
                     If False, use official rates.

    Returns:
        Payment rate in kr/ha, or None if not found

    Examples:
        >>> get_expected_rate("OEKOLOGISK_36")
        868
        >>> get_expected_rate("OEKOLOGISK_36", use_derived=False)
        955
    """
    rates = DERIVED_PAYMENT_RATES if use_derived else OFFICIAL_PAYMENT_RATES
    return rates.get(ordning_kode)


def is_summary_row(scheme_name: str) -> bool:
    """
    Check if a scheme name indicates a summary/total row.

    These rows must be filtered out to avoid double-counting.

    Args:
        scheme_name: Scheme name from støtteoplysninger

    Returns:
        True if this is a summary row that should be excluded

    Examples:
        >>> is_summary_row("Total for beneficiary")
        True
        >>> is_summary_row("Basic payment scheme")
        False
    """
    if not scheme_name:
        return False

    summary_indicators = [
        "total for beneficiary",
        "total",
        "sum",
        "i alt",
        "subtotal",
    ]

    scheme_lower = scheme_name.lower().strip()
    return any(indicator in scheme_lower for indicator in summary_indicators)


def get_pillar(ordning_kode: str) -> int | None:
    """
    Get the CAP pillar for a scheme (1 = EAGF, 2 = EAFRD).

    Args:
        ordning_kode: Standardized scheme code

    Returns:
        1 for Pillar 1 (direct payments), 2 for Pillar 2 (rural development), None if unknown
    """
    pillar1 = {
        "GRUNDBETALING",
        "GROEN_STOETTE",
        "KOBLET_STOETTE",
        "OMFORDELING",
        "UNGE_LANDMAEND",
        "SMAA_LANDMAEND",
    }
    pillar2 = {
        "OEKOLOGISK",
        "GRAESPLEJE",
        "NATURA2000",
        "NATURLIGE_BEGR",
        "LANDBRUG_UDVIKLING",
        "DYREVELFAERD",
    }

    # Check base code (without suffix like _36, _67)
    base_code = ordning_kode.split("_")[0] if "_" in ordning_kode else ordning_kode

    if base_code in pillar1 or ordning_kode in pillar1:
        return 1
    if base_code in pillar2 or ordning_kode in pillar2:
        return 2

    # Check if it starts with a pillar 2 prefix
    for p2 in pillar2:
        if ordning_kode.startswith(p2):
            return 2

    return None
