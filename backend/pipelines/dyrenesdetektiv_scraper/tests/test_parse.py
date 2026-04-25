"""Parser tests for dyrenesdetektiv_scraper silver stage."""

import re

from silver.parse import SANKTION_ORDINAL, _parse_sanktion, parse_detail_html

# Slug → display name lookup mirroring the kontrol_tag taxonomy. Only entries
# referenced by the captured fixtures need to be present.
SLUG_LOOKUP = {
    "2022": "2022",
    "ikast-brande": "Ikast-Brande",
    "hoens-af-aeglaegningstype": "Høns af æglægningstype",
    "svin": "Svin",
    "hunde": "Hunde",
}


class TestFullRecord:
    """Record with all fields populated (CHR + CVR + numeric Antal dyr)."""

    def test_extracts_kontrol_id_from_body_class(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["kontrol_id"] == 13758

    def test_extracts_chr_as_six_digit_string(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["chr_nummer"] == "068738"
        assert re.fullmatch(r"\d{6}", record["chr_nummer"])

    def test_extracts_cvr_as_eight_digit_string(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["cvr_nummer"] == "67669258"
        assert re.fullmatch(r"\d{8}", record["cvr_nummer"])

    def test_parses_kontrol_dato_to_iso(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["kontrol_dato"] == "2022-01-12"

    def test_extracts_dyreart(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert "Høns" in record["dyreart"]

    def test_parses_antal_dyr_with_thousands_separator(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["antal_dyr"] == 23345

    def test_extracts_by(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["by"] == "Ejstrupholm / Ikast-Brande"

    def test_sanktion_text_strips_legend(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["sanktion"] == "Ingen anmærkninger"
        assert record["sanktion_ordinal"] == 1

    def test_kontroltekst_captured_inline(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert "velfærdskontrol" in record["kontroltekst"]
        assert "Egenkontrol" in record["kontroltekst"]
        # The "Kilde:" footer must NOT bleed into the narrative.
        assert "Kilde:" not in record["kontroltekst"]

    def test_link_present(self, full_record_html):
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["link"].endswith(
            "kontrol-2022-besaetning-23345-hoens-af-aeglaegningstype-ikast-brande/"
        )


class TestSlaughterhouseRedacted:
    """Slaughterhouse record where CHR/CVR/By/Antal dyr are blank."""

    def test_chr_nummer_is_none_when_blank(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["chr_nummer"] is None

    def test_cvr_nummer_is_none_when_blank(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["cvr_nummer"] is None

    def test_antal_dyr_is_none_when_blank(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["antal_dyr"] is None

    def test_kontrol_dato_is_none_when_blank(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["kontrol_dato"] is None

    def test_sagsnummer_extracted(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["sagsnummer"] == "2022-10-721-111673"

    def test_sanktion_indskaerpelse_maps_to_two(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["sanktion"] == "Indskærpelse"
        assert record["sanktion_ordinal"] == 2

    def test_kontroltekst_handles_multiple_paragraphs(self, slagteri_html):
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        # Slagteri narrative spans multiple <p> blocks; all must be joined.
        assert "transportegnethed" in record["kontroltekst"]


class TestNumericSanktion:
    """Record where sanktion is given as a bare number (e.g. '2 (2 = Indskærpelse)')."""

    def test_numeric_sanktion_resolves_via_legend(self, hunde_html):
        record = parse_detail_html(hunde_html, SLUG_LOOKUP)
        # The bare numeric value "2" is resolved through the inline legend.
        assert record["sanktion_ordinal"] == 2

    def test_aarsag_extracted_when_present(self, hunde_html):
        record = parse_detail_html(hunde_html, SLUG_LOOKUP)
        assert "Frekvensbaseret" in record["aarsag"]


class TestSanktionMapping:
    """Static mapping from sanktion text/code to ordinal severity."""

    def test_known_codes(self):
        assert SANKTION_ORDINAL["Ingen anmærkninger"] == 1
        assert SANKTION_ORDINAL["Indskærpelse"] == 2
        assert SANKTION_ORDINAL["Politianmeldelse"] == 3
        assert SANKTION_ORDINAL["Bøde"] == 4

    def test_legend_provides_ordinal_when_head_unrecognized(self):
        # Real records exist where the head is the literal placeholder "Sanktion"
        # or a free-form value like "Påbud" — the legend still encodes the ordinal.
        assert _parse_sanktion("Sanktion (2 = Indskærpelse)") == ("Sanktion", 2)
        assert _parse_sanktion("Påbud (2 = Indskærpelse)") == ("Påbud", 2)
        assert _parse_sanktion("Påbud/Forbud (2 = Indskærpelse)") == ("Påbud/Forbud", 2)

    def test_legend_ignored_when_head_matches_known_label(self):
        # Known labels keep their canonical ordinal even if the legend says otherwise.
        assert _parse_sanktion("Bøde (2 = Indskærpelse)") == ("Bøde", 4)


class TestTagBucketing:
    """Article-class tag slugs are bucketed into year / kommune / dyreart."""

    def test_full_record_resolves_kommune_and_dyreart(self, full_record_html):
        # Article class is "tag-1215 tag-hoens-af-aeglaegningstype tag-ikast-brande".
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["tag_kommune"] == "Ikast-Brande"
        assert record["tag_dyreart"] == "Høns af æglægningstype"

    def test_year_falls_back_to_url_slug_when_not_tagged(self, full_record_html):
        # No "2022" tag on the article element; URL slug starts with "kontrol-2022-".
        record = parse_detail_html(full_record_html, SLUG_LOOKUP)
        assert record["tag_year"] == "2022"

    def test_dyreart_recognized_for_slagteri(self, slagteri_html):
        # Article class includes "tag-svin" — this should resolve to dyreart "Svin".
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["tag_dyreart"] == "Svin"

    def test_unknown_slugs_silently_dropped(self, slagteri_html):
        # Slagteri article also has tag-dyrevelfaerdsloven / tag-foedevarestyrelsen
        # / tag-grise — none in our taxonomy; they must not become kommune.
        record = parse_detail_html(slagteri_html, SLUG_LOOKUP)
        assert record["tag_kommune"] is None

    def test_dyreart_recognized_for_hunde(self, hunde_html):
        record = parse_detail_html(hunde_html, SLUG_LOOKUP)
        assert record["tag_dyreart"] == "Hunde"

    def test_empty_lookup_is_safe(self, full_record_html):
        record = parse_detail_html(full_record_html, {})
        # No taxonomy → no kommune/dyreart, but year still resolved from URL.
        assert record["tag_kommune"] is None
        assert record["tag_dyreart"] is None
        assert record["tag_year"] == "2022"
