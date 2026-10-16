"""No-network checks for source fetch validation and silver source parsers."""

from __future__ import annotations

import asyncio
import base64
import csv
import json
from collections import Counter
from pathlib import Path

import pytest

from child_receptors.bronze import bbr_playgrounds, dagtilbud, geofa_playgrounds, osm_playgrounds, stil_institutions
from child_receptors.silver.build import parse_bbr, parse_dagtilbud, parse_geofa, parse_osm, parse_stil


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=headers, delimiter=";", lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)


def test_dagtilbud_parser_maps_types_ownership_and_alternative_addresses(tmp_path: Path) -> None:
    institutions = tmp_path / "daginstitution.csv"
    sites = tmp_path / "anvisningsenhed.csv"
    alternatives = tmp_path / "alternativ_adresse.csv"
    _write_csv(
        institutions,
        ["daginstitutionsNummer", "ejerformKode", "ejerformKode_Tekst"],
        [
            {"daginstitutionsNummer": "D1", "ejerformKode": "1", "ejerformKode_Tekst": "Kommunal"},
            {"daginstitutionsNummer": "D2", "ejerformKode": "2", "ejerformKode_Tekst": "Selvejende"},
            {"daginstitutionsNummer": "D3", "ejerformKode": "3", "ejerformKode_Tekst": "Private"},
        ],
    )
    headers = [
        "anvisningsenhedsNummer",
        "daginstitutionsNummer",
        "instType3",
        "aktivitetsstatus",
        "navn",
        "kommuneKode",
        "vejNavn",
        "husNummer",
        "postNummer",
        "byNavn",
        "dawaId",
        "cvr",
        "pNumber",
        "geoBredde",
        "geoLaengde",
        "senestOpdateret",
    ]
    _write_csv(
        sites,
        headers,
        [
            {
                "anvisningsenhedsNummer": "A1",
                "daginstitutionsNummer": "D1",
                "instType3": "6010",
                "aktivitetsstatus": "Aktiv",
                "navn": "Børnehuset",
                "kommuneKode": "210",
                "vejNavn": "Testvej",
                "husNummer": "4A",
                "postNummer": "3400",
                "byNavn": "Hillerød",
                "dawaId": "dawa-1",
                "cvr": "1234567",
                "pNumber": "123456789",
                "geoBredde": "55.9300",
                "geoLaengde": "12.3000",
                "senestOpdateret": "2026-01-01",
            },
            {
                "anvisningsenhedsNummer": "A2",
                "daginstitutionsNummer": "D2",
                "instType3": "6022",
                "aktivitetsstatus": "Aktiv",
                "navn": "Særligt tilbud",
                "kommuneKode": "147",
                "geoBredde": "55.6800",
                "geoLaengde": "12.5600",
            },
            {
                "anvisningsenhedsNummer": "A6",
                "daginstitutionsNummer": "D3",
                "instType3": "6014",
                "aktivitetsstatus": "Aktiv",
                "navn": "Privat børnehave",
                "kommuneKode": "480",
                "cvr": "ikke-et-cvr",
                "pNumber": "12X",
                "geoBredde": "55.0000",
                "geoLaengde": "10.0000",
            },
            {
                "anvisningsenhedsNummer": "A3",
                "daginstitutionsNummer": "D3",
                "instType3": "6013",
                "aktivitetsstatus": "Aktiv",
                "geoBredde": "55.68",
                "geoLaengde": "12.56",
            },
            {
                "anvisningsenhedsNummer": "A4",
                "daginstitutionsNummer": "D1",
                "instType3": "6011",
                "aktivitetsstatus": "Nedlagt",
                "geoBredde": "55.68",
                "geoLaengde": "12.56",
            },
            {
                "anvisningsenhedsNummer": "A5",
                "daginstitutionsNummer": "D3",
                "instType3": "6021",
                "aktivitetsstatus": "Aktiv",
                "geoBredde": "",
                "geoLaengde": "",
            },
        ],
    )
    _write_csv(
        alternatives,
        [
            "alternativAdresseNummer",
            "anvisningsenhedsNummer",
            "geoBredde",
            "geoLaengde",
            "kommuneKode",
            "vejNavn",
            "husNummer",
            "postNummer",
            "byNavn",
        ],
        [
            {
                "alternativAdresseNummer": "ALT1",
                "anvisningsenhedsNummer": "A1",
                "geoBredde": "55.9310",
                "geoLaengde": "12.3010",
                "kommuneKode": "210",
                "vejNavn": "Sidevej",
                "husNummer": "2",
                "postNummer": "3400",
                "byNavn": "Hillerød",
            },
            {
                "alternativAdresseNummer": "ALT2",
                "anvisningsenhedsNummer": "A3",
                "geoBredde": "55.68",
                "geoLaengde": "12.56",
            },
        ],
    )

    dropped = Counter()
    warnings = Counter()
    rows = parse_dagtilbud(
        sites,
        institutions,
        alternatives,
        dropped=dropped,
        warnings=warnings,
        fetch_timestamp="fetch-time",
    )
    by_id = {row["receptor_id"]: row for row in rows}
    assert set(by_id) == {"dtr:A1", "dtr:A2", "dtr:A6", "dtr-alt:ALT1"}
    assert by_id["dtr:A1"]["kommune_kode"] == "0210"
    assert by_id["dtr:A1"]["ownership"] == "Kommunal"
    assert by_id["dtr:A1"]["cvr"] == "01234567"
    assert by_id["dtr:A1"]["p_number"] == "0123456789"
    assert by_id["dtr:A1"]["address"] == "Testvej 4A, 3400 Hillerød"
    assert by_id["dtr:A2"]["receptor_type"] == "special_daycare"
    assert by_id["dtr:A2"]["ownership"] == "Selvejende"
    assert by_id["dtr:A6"]["ownership"] == "Private"
    assert by_id["dtr:A6"]["cvr"] is None
    assert by_id["dtr:A6"]["p_number"] is None
    assert warnings["invalid_cvr_format"] == 1
    assert warnings["invalid_p_number_format"] == 1
    assert by_id["dtr-alt:ALT1"]["subtype"].endswith(" – alternativ adresse")
    assert by_id["dtr-alt:ALT1"]["ownership"] == "Kommunal"
    assert by_id["dtr-alt:ALT1"]["kommune_kode"] == "0210"
    assert dropped["dagtilbud_dagpleje_excluded"] == 1
    assert dropped["dagtilbud_inactive"] == 1
    assert dropped["dagtilbud_missing_coordinates"] == 1
    assert dropped["alternative_address_inactive_or_excluded_parent"] == 1


def test_unknown_active_dagtilbud_type_fails(tmp_path: Path) -> None:
    institution_path = tmp_path / "institution.csv"
    site_path = tmp_path / "site.csv"
    _write_csv(institution_path, ["daginstitutionsNummer", "ejerformKode"], [])
    _write_csv(
        site_path,
        ["anvisningsenhedsNummer", "instType3", "aktivitetsstatus"],
        [{"anvisningsenhedsNummer": "A1", "instType3": "9999", "aktivitetsstatus": "Aktiv"}],
    )
    with pytest.raises(ValueError, match="Unknown active Dagtilbud instType3"):
        parse_dagtilbud(site_path, institution_path)


def test_stil_parser_filters_active_types_and_maps_municipalities(fixtures_dir: Path, tmp_path: Path) -> None:
    dropped = Counter()
    warnings = Counter()
    rows = parse_stil(
        fixtures_dir / "stil_institutions.sample.json",
        dropped=dropped,
        warnings=warnings,
        fetch_timestamp="stil-fetch",
    )
    by_id = {row["receptor_id"]: row for row in rows}
    assert by_id["stil:101001"]["receptor_type"] == "school"
    assert by_id["stil:101001"]["kommune_kode"] == "0101"
    assert by_id["stil:101001"]["address"] == "Vester Voldgade 98, 1552 København V"
    assert by_id["stil:101001"]["ownership"] == "Kommunale"
    assert "stil:101002" not in by_id
    assert "stil:101000" not in by_id
    assert by_id["stil:101093"]["receptor_type"] == "special_school"
    assert by_id["stil:101303"]["receptor_type"] == "boarding_school"
    assert dropped["stil_inactive"] > 0
    assert dropped["stil_excluded_type"] > 0


def test_bbr_reduces_bitemporal_rows_and_filters_status(fixtures_dir: Path, tmp_path: Path) -> None:
    payload = json.loads((fixtures_dir / "bbr_1905.sample.json").read_text(encoding="utf-8"))
    payload.extend(
        [
            {
                "id_lokalId": "inactive",
                "kommunekode": "0147",
                "status": "5",
                "registreringFra": "2024-01-01T00:00:00Z",
                "virkningFra": "2024-01-01T00:00:00Z",
                "tek109Koordinat": {"wkt": "POINT (721000 6176000)"},
            },
            {
                "id_lokalId": "latest-status",
                "kommunekode": "0147",
                "status": "6",
                "registreringFra": "2020-01-01T00:00:00Z",
                "virkningFra": "2020-01-01T00:00:00Z",
                "tek109Koordinat": {"wkt": "POINT (721000 6176000)"},
            },
            {
                "id_lokalId": "latest-status",
                "kommunekode": "0147",
                "status": "8",
                "registreringFra": "2021-01-01T00:00:00Z",
                "virkningFra": "2021-01-01T00:00:00Z",
                "tek109Koordinat": {"wkt": "POINT (721100 6176100)"},
            },
        ]
    )
    path = tmp_path / "bbr.jsonl"
    path.write_text("\n".join(json.dumps(item) for item in payload), encoding="utf-8")

    dropped = Counter()
    rows = parse_bbr(path, dropped=dropped)
    latest = next(row for row in rows if row["receptor_id"] == "bbr:37a644d5-d54d-4fbf-8d99-dec9f05601a4")
    assert latest["utm_e"] == pytest.approx(721276.05)
    assert latest["source_updated_at"] == "2018-04-24T10:44:00.343273Z"
    assert "bbr:inactive" not in {row["receptor_id"] for row in rows}
    assert "bbr:latest-status" not in {row["receptor_id"] for row in rows}
    assert dropped["bbr_status_excluded"] == 2


def test_geofa_filters_and_uses_first_multipoint(fixtures_dir: Path, tmp_path: Path) -> None:
    payload = json.loads((fixtures_dir / "geofa_t5800.sample.json").read_text(encoding="utf-8"))
    payload["features"][0]["geometry"]["coordinates"].append([725900.0, 6166600.0])
    payload["features"].append(
        {
            "type": "Feature",
            "properties": {"objekt_id": "bad-status", "facil_ty_k": 1031, "statuskode": 2},
            "geometry": {"type": "MultiPoint", "coordinates": [[700000, 6100000]]},
        }
    )
    path = tmp_path / "geofa.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    dropped = Counter()
    rows = parse_geofa(path, dropped=dropped)
    point = next(row for row in rows if row["receptor_id"] == "geofa:7993bb56-e866-11ef-85f5-b3a8e5a00bbf")
    assert point["utm_e"] == pytest.approx(725846.4224814365)
    assert point["utm_n"] == pytest.approx(6166499.948562623)
    assert point["subtype"] == "Naturlegeplads"
    assert dropped["geofa_type_or_status_excluded"] == 2


def test_osm_node_way_private_and_cross_region_duplicate(tmp_path: Path) -> None:
    first = tmp_path / "osm_playgrounds_region_a.json"
    second = tmp_path / "osm_playgrounds_region_b.json"
    first.write_text(
        json.dumps(
            {
                "elements": [
                    {
                        "type": "node",
                        "id": 1,
                        "lat": 55.6761,
                        "lon": 12.5683,
                        "tags": {"leisure": "playground", "name": "Node"},
                    },
                    {
                        "type": "way",
                        "id": 2,
                        "center": {"lat": 55.677, "lon": 12.569},
                        "tags": {"leisure": "playground"},
                    },
                    {
                        "type": "node",
                        "id": 3,
                        "lat": 55.678,
                        "lon": 12.57,
                        "tags": {"leisure": "playground", "access": "private"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    second.write_text(
        json.dumps(
            {
                "elements": [
                    {"type": "node", "id": 1, "lat": 55.6761, "lon": 12.5683, "tags": {"leisure": "playground"}},
                    {
                        "type": "relation",
                        "id": 4,
                        "center": {"lat": 55.679, "lon": 12.571},
                        "tags": {"leisure": "playground", "access": "customers"},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    dropped = Counter()
    rows = parse_osm([first, second], dropped=dropped)
    by_id = {row["receptor_id"]: row for row in rows}
    assert set(by_id) == {"osm:node/1", "osm:way/2", "osm:relation/4"}
    assert by_id["osm:node/1"]["name"] == "Node"
    assert by_id["osm:way/2"]["lat"] == pytest.approx(55.677)
    assert dropped["osm_duplicate_region_element"] == 1
    assert dropped["osm_private_access"] == 1


def test_dagtilbud_validator_rejects_html_and_accepts_mock_playwright() -> None:
    html = b"<!DOCTYPE html><html><body>F5 challenge</body></html>"
    incomplete = dict.fromkeys(dagtilbud.EXPECTED_COLUMNS, html)
    with pytest.raises(ValueError, match="unexpected header"):
        dagtilbud.validate_exports(incomplete)

    bodies = {
        f"dagtilbudsregister_{name}.csv": column.encode() + b";rest\r\nrow;1\r\n"
        for name, column in dagtilbud.EXPECTED_COLUMNS.items()
    }
    bodies["dagtilbudsregister_anvisningsenhed.csv"] = b"anvisningsenhedsNummer;rest\r\n" + b"row;1\r\n" * 4000
    exports = {name: b"\xef\xbb\xbf" + body for name, body in bodies.items()}

    class FakePage:
        async def goto(self, *_args, **_kwargs):
            return None

        async def content(self):
            return "Eksport af data"

        async def evaluate(self, _script, target):
            export_name = next(name for name, control in dagtilbud.TARGETS.items() if control == target)
            return {"status": 200, "b64": base64.b64encode(exports[f"dagtilbudsregister_{export_name}.csv"]).decode()}

    class FakeBrowser:
        async def new_page(self, **_kwargs):
            return FakePage()

        async def close(self):
            return None

    class FakeChromium:
        async def launch(self, **_kwargs):
            return FakeBrowser()

    class FakePlaywright:
        chromium = FakeChromium()

    class FakePlaywrightContext:
        async def __aenter__(self):
            return FakePlaywright()

        async def __aexit__(self, *_args):
            return None

    result = asyncio.run(dagtilbud._fetch_session(lambda: FakePlaywrightContext()))
    assert set(result) == {f"dagtilbudsregister_{name}.csv" for name in dagtilbud.TARGETS}


def test_http_fetchers_validate_mocked_responses() -> None:
    institutions = json.dumps([{}] * 5000).encode()

    class FakeResponse:
        content = institutions

        def raise_for_status(self):
            return None

    result = stil_institutions.fetch(get=lambda _url, **_kwargs: FakeResponse())
    assert len(json.loads(result["institutions.json"])) == 5000

    with pytest.raises(ValueError, match="at least 5000"):
        stil_institutions.validate_institutions(b"[]")
    with pytest.raises(ValueError, match="at least 20000"):
        geofa_playgrounds.validate_geofa(b'{"type":"FeatureCollection","features":[]}')
    with pytest.raises(ValueError, match="expected at least 300"):
        osm_playgrounds.validate_region(b'{"elements":[]}', "Region Hovedstaden")


def test_osm_fetch_tries_fallback_endpoints_in_order() -> None:
    calls: list[str] = []
    delays: list[float] = []
    body = json.dumps({"elements": [{}] * 300}).encode()

    class FakeResponse:
        content = body

        def raise_for_status(self) -> None:
            return None

    def post(url: str, **_kwargs):
        calls.append(url)
        if url != osm_playgrounds.ENDPOINTS[2]:
            raise RuntimeError("gateway timeout")
        return FakeResponse()

    files = osm_playgrounds.fetch(
        regions=("Region Hovedstaden",),
        post=post,
        backoff=delays.append,
    )

    assert calls == [osm_playgrounds.ENDPOINTS[0]] * 4 + [osm_playgrounds.ENDPOINTS[1]] * 4 + [
        osm_playgrounds.ENDPOINTS[2]
    ]
    assert delays == list(osm_playgrounds.BACKOFF_SECONDS) * 2
    assert list(files) == ["osm_playgrounds_hovedstaden.json"]


def test_bbr_query_uses_cursor_and_kommunekode() -> None:
    query = bbr_playgrounds.make_query("0147", "2026-10-10T12:00:00Z", "next-cursor")
    assert 'after: "next-cursor"' in query
    assert 'kommunekode: { eq: "0147" }' in query
    assert 'tek020Klassifikation: { eq: "1905" }' in query


def test_bbr_fetch_uses_mocked_cursor_pages_without_network() -> None:
    calls = []

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def mocked_post(_url, **kwargs):
        query = kwargs["json"]["query"]
        calls.append(query)
        if len(calls) == 1:
            return FakeResponse(
                {
                    "data": {
                        "BBR_TekniskAnlaeg": {
                            "nodes": [{"id_lokalId": "one"}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                        }
                    }
                }
            )
        return FakeResponse(
            {
                "data": {
                    "BBR_TekniskAnlaeg": {
                        "nodes": [{"id_lokalId": "two"}],
                        "pageInfo": {"hasNextPage": False, "endCursor": "cursor-2"},
                    }
                }
            }
        )

    nodes = bbr_playgrounds.fetch_kommune(
        "0101",
        api_key="test-key",
        virkningstid="2026-10-10T12:00:00Z",
        post=mocked_post,
        backoff=lambda _delay: None,
    )
    assert [node["id_lokalId"] for node in nodes] == ["one", "two"]
    assert len(calls) == 2
    assert "after:" not in calls[0]
    assert 'after: "cursor-1"' in calls[1]


def test_bbr_errors_never_leak_api_key() -> None:
    class ErrorResponse:
        status_code = 400
        text = "Bad Request for url https://graphql.datafordeler.dk/BBR/v2?apikey=secret-key"

    with pytest.raises(RuntimeError) as excinfo:
        bbr_playgrounds.fetch_kommune(
            "0101",
            api_key="secret-key",
            virkningstid="2026-10-10T12:00:00Z",
            post=lambda _url, **_kwargs: ErrorResponse(),
            backoff=lambda _delay: None,
        )
    chain = []
    error = excinfo.value
    while error is not None:
        chain.append(str(error))
        error = error.__cause__ or error.__context__
    assert all("secret-key" not in message for message in chain)


def test_osm_region_slugs_transliterate_danish_letters() -> None:
    from child_receptors.config import OSM_REGIONS

    slugs = {osm_playgrounds.region_slug(region) for region in OSM_REGIONS}
    assert slugs == {"hovedstaden", "sjaelland", "syddanmark", "midtjylland", "nordjylland"}
