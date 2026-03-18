#!/usr/bin/env python3
"""
Proof-of-concept: Test Datafordeleren GraphQL API for GeoDanmark + BBR data.

Tests whether we can replace the broken GeoDanmark WFS with GraphQL queries
from Datafordeleren's GEODKV and BBR registers.

Confirmed findings from official GraphQL schemas (fetched via /schema endpoint):

GEODKV_Bygning (37 fields):
  - id_lokalId (String!), BBRUUID (String), bygningstype (String!)
  - geometri (SpatialPolygonZEpsg25832Type) → subfields: { wkt, type, crs, dimension }
  - status, geometristatus, BBRaktion, maalestedBygning, metode3D
  - overlapBygning (Boolean!), synligBygning (Boolean!), underMinimumBygning (Boolean!)
  - registreringFra/Til, virkningFra/Til, forretningshaendelse

BBR_Bygning (94 fields):
  - id_lokalId (String!), kommunekode, byg021BygningensAnvendelse
  - byg404Koordinat (SpatialPointEpsg25832Type) → subfields: { wkt, type, crs, dimension }
  - byg007Bygningsnummer, byg026Opfoerelsesaar, byg038SamletBygningsareal
  - byg041BebyggetAreal, byg054AntalEtager, byg056Varmeinstallation
  - grund, husnummer, jordstykke, ejerlejlighed
  - Full list: see .context/BBR_schema.graphql

API details:
  - Endpoints: https://graphql.datafordeler.dk/{GEODKV,BBR}/v1?apiKey=<key>
  - Auth: API key + IP whitelisting required
  - Pagination: cursor-based, first (max 1000) + after
  - Bitemporal: registreringstid required (or id_lokalId filter)
  - Spatial filter: geometri: { intersects: { wkt: "POLYGON(...)", crs: 25832 } }
  - Cross-reference: GEODKV_Bygning.BBRUUID == BBR_Bygning.id_lokalId

Usage:
    DATAFORDELER_GRAPHQL_API_KEY=<key> python bronze/test_graphql_datafordeler.py
"""

import json
import os
import sys
from datetime import datetime, timezone

import requests

API_KEY = os.getenv("DATAFORDELER_GRAPHQL_API_KEY")
if not API_KEY:
    print("ERROR: Set DATAFORDELER_GRAPHQL_API_KEY environment variable")
    sys.exit(1)

GEODKV_URL = f"https://graphql.datafordeler.dk/GEODKV/v1?apiKey={API_KEY}"
BBR_URL = f"https://graphql.datafordeler.dk/BBR/v1?apiKey={API_KEY}"

NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def graphql_query(url: str, query: str, label: str = "") -> dict:
    """Execute a GraphQL query and return the response."""
    resp = requests.post(
        url,
        headers={"Content-Type": "application/json; charset=utf-8"},
        json={"query": query},
        timeout=60,
    )
    if resp.status_code != 200:
        print(f"  [{label}] HTTP {resp.status_code}: {resp.text[:300]}")
        return {}
    data = resp.json()
    if "errors" in data:
        errors_brief = [e.get("message", "?")[:120] for e in data["errors"]]
        print(f"  [{label}] GraphQL errors: {errors_brief}")
    return data


def test_geodkv_buildings() -> list[dict]:
    """Query GEODKV_Bygning with geometry."""
    section("GEODKV: Query Buildings with Geometry")

    query = f"""
    {{
      GEODKV_Bygning(
        registreringstid: "{NOW}",
        first: 5
      ) {{
        nodes {{
          id_lokalId
          BBRUUID
          bygningstype
          status
          geometristatus
          overlapBygning
          synligBygning
          underMinimumBygning
          geometri {{
            wkt
            type
            crs
            dimension
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    data = graphql_query(GEODKV_URL, query, "geodkv-buildings")
    nodes = data.get("data", {}).get("GEODKV_Bygning", {}).get("nodes", [])
    page_info = data.get("data", {}).get("GEODKV_Bygning", {}).get("pageInfo", {})

    print(f"  Got {len(nodes)} buildings")
    print(f"  hasNextPage: {page_info.get('hasNextPage')}")
    for node in nodes[:3]:
        geom = node.get("geometri", {})
        wkt = geom.get("wkt", "")
        print(f"\n  Building id={node.get('id_lokalId')}, BBRUUID={node.get('BBRUUID')}")
        print(f"    type={node.get('bygningstype')}, status={node.get('status')}")
        print(
            f"    geom.type={geom.get('type')}, crs={geom.get('crs')}, dim={geom.get('dimension')}"
        )
        print(f"    wkt={wkt[:120]}{'...' if len(wkt) > 120 else ''}")
    return nodes


def test_geodkv_by_bbruuid(bbruuid: str) -> dict | None:
    """Query a specific GEODKV building by BBRUUID."""
    section(f"GEODKV: Query by BBRUUID {bbruuid[:30]}...")

    query = f"""
    {{
      GEODKV_Bygning(
        registreringstid: "{NOW}",
        where: {{ BBRUUID: {{ eq: "{bbruuid}" }} }}
      ) {{
        nodes {{
          id_lokalId
          BBRUUID
          bygningstype
          status
          geometri {{
            wkt
            type
          }}
        }}
      }}
    }}
    """
    data = graphql_query(GEODKV_URL, query, "geodkv-by-bbruuid")
    nodes = data.get("data", {}).get("GEODKV_Bygning", {}).get("nodes", [])
    if nodes:
        node = nodes[0]
        print(f"  Found: {json.dumps(node, indent=4, default=str)[:500]}")
        return node
    print("  Not found")
    return None


def test_geodkv_spatial_filter() -> list[dict]:
    """Test spatial filtering on GEODKV — query buildings within a bounding box."""
    section("GEODKV: Spatial Filter Test (Copenhagen area)")

    # Small area in central Copenhagen (EPSG:25832 coordinates)
    # The spatial filter input requires { wkt: "..." } not just a string
    bbox_wkt = (
        "POLYGON((723000 6175000, 724000 6175000, 724000 6176000, 723000 6176000, 723000 6175000))"
    )

    query = f"""
    {{
      GEODKV_Bygning(
        registreringstid: "{NOW}",
        where: {{ geometri: {{ intersects: {{ wkt: "{bbox_wkt}", crs: 25832 }} }} }},
        first: 5
      ) {{
        nodes {{
          id_lokalId
          BBRUUID
          bygningstype
          geometri {{
            wkt
            type
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    data = graphql_query(GEODKV_URL, query, "geodkv-spatial")
    nodes = data.get("data", {}).get("GEODKV_Bygning", {}).get("nodes", [])
    page_info = data.get("data", {}).get("GEODKV_Bygning", {}).get("pageInfo", {})

    print(f"  Got {len(nodes)} buildings in Copenhagen bbox")
    print(f"  hasNextPage: {page_info.get('hasNextPage')}")
    for node in nodes[:2]:
        wkt = node.get("geometri", {}).get("wkt", "")
        print(f"  id={node.get('id_lokalId')}, BBRUUID={node.get('BBRUUID')}")
        print(f"    wkt={wkt[:120]}...")
    return nodes


def test_bbr_buildings() -> list[dict]:
    """Query BBR_Bygning with coordinates."""
    section("BBR: Query Buildings with Coordinates")

    query = f"""
    {{
      BBR_Bygning(
        registreringstid: "{NOW}",
        first: 5
      ) {{
        nodes {{
          id_lokalId
          kommunekode
          byg021BygningensAnvendelse
          byg007Bygningsnummer
          byg026Opfoerelsesaar
          byg038SamletBygningsareal
          byg041BebyggetAreal
          byg054AntalEtager
          grund
          husnummer
          jordstykke
          status
          byg404Koordinat {{
            wkt
            type
          }}
        }}
        pageInfo {{
          hasNextPage
          endCursor
        }}
      }}
    }}
    """
    data = graphql_query(BBR_URL, query, "bbr-buildings")
    nodes = data.get("data", {}).get("BBR_Bygning", {}).get("nodes", [])
    page_info = data.get("data", {}).get("BBR_Bygning", {}).get("pageInfo", {})

    print(f"  Got {len(nodes)} BBR buildings")
    print(f"  hasNextPage: {page_info.get('hasNextPage')}")
    for node in nodes[:3]:
        coord = node.get("byg404Koordinat", {})
        print(f"\n  Building {node.get('id_lokalId')}")
        print(
            f"    anvendelse={node.get('byg021BygningensAnvendelse')}, kommune={node.get('kommunekode')}"
        )
        print(
            f"    areal={node.get('byg038SamletBygningsareal')}m2, etager={node.get('byg054AntalEtager')}"
        )
        print(f"    coord={coord.get('wkt', 'N/A')}")
    return nodes


def test_bbr_by_uuid(uuid: str) -> dict | None:
    """Query a specific BBR building by id_lokalId."""
    section(f"BBR: Query by UUID {uuid[:30]}...")

    query = f"""
    {{
      BBR_Bygning(
        registreringstid: "{NOW}",
        where: {{ id_lokalId: {{ eq: "{uuid}" }} }}
      ) {{
        nodes {{
          id_lokalId
          kommunekode
          byg021BygningensAnvendelse
          byg038SamletBygningsareal
          byg041BebyggetAreal
          byg054AntalEtager
          byg404Koordinat {{
            wkt
            type
          }}
        }}
      }}
    }}
    """
    data = graphql_query(BBR_URL, query, "bbr-by-uuid")
    nodes = data.get("data", {}).get("BBR_Bygning", {}).get("nodes", [])
    if nodes:
        print(f"  Found: {json.dumps(nodes[0], indent=4)}")
        return nodes[0]
    print("  Not found")
    return None


def test_bbr_batch_query(uuids: list[str]) -> list[dict]:
    """Test batch querying BBR buildings by UUID list (replicates pipeline pattern)."""
    section(f"BBR: Batch Query ({len(uuids)} UUIDs)")

    uuids_list = '["' + '","'.join(uuids) + '"]'
    query = f"""
    {{
      BBR_Bygning(
        registreringstid: "{NOW}",
        where: {{ id_lokalId: {{ in: {uuids_list} }} }}
      ) {{
        nodes {{
          id_lokalId
          byg021BygningensAnvendelse
          kommunekode
          byg041BebyggetAreal
        }}
      }}
    }}
    """
    data = graphql_query(BBR_URL, query, "bbr-batch")
    nodes = data.get("data", {}).get("BBR_Bygning", {}).get("nodes", [])
    print(f"  Queried {len(uuids)} UUIDs, got {len(nodes)} results")
    for node in nodes[:3]:
        print(f"  {node.get('id_lokalId')}: code={node.get('byg021BygningensAnvendelse')}")
    return nodes


def test_cross_reference(geodkv_nodes: list[dict]) -> bool:
    """Cross-reference: look up GEODKV buildings in BBR via BBRUUID."""
    # Find a GEODKV node with a BBRUUID
    bbruuid = None
    for node in geodkv_nodes:
        if node.get("BBRUUID"):
            bbruuid = node["BBRUUID"]
            break

    if not bbruuid:
        print("  No BBRUUID found in GEODKV results to cross-reference")
        return False

    section(f"Cross-Reference: BBRUUID {bbruuid}")

    # Look up in BBR
    bbr_node = test_bbr_by_uuid(bbruuid)

    # Look up same building in GEODKV by BBRUUID
    geodkv_node = test_geodkv_by_bbruuid(bbruuid)

    if bbr_node and geodkv_node:
        print("\n  CONFIRMED: GEODKV_Bygning.BBRUUID == BBR_Bygning.id_lokalId")
        print(f"  GEODKV geometry: {geodkv_node.get('geometri', {}).get('type', 'N/A')}")
        print(
            f"  BBR details: code={bbr_node.get('byg021BygningensAnvendelse')}, "
            f"areal={bbr_node.get('byg038SamletBygningsareal')}m2"
        )
        return True
    return False


def main() -> None:
    print("Datafordeleren GraphQL API - Proof of Concept")
    print(f"Timestamp: {NOW}")

    # --- Test 1: GEODKV buildings with polygon geometry ---
    geodkv_nodes = test_geodkv_buildings()

    # --- Test 2: GEODKV spatial filter (query by bounding box) ---
    spatial_nodes = test_geodkv_spatial_filter()

    # --- Test 3: BBR buildings with point coordinates ---
    bbr_nodes = test_bbr_buildings()

    # --- Test 4: Cross-reference GEODKV <-> BBR ---
    cross_ref_ok = False
    if geodkv_nodes:
        cross_ref_ok = test_cross_reference(geodkv_nodes)

    # --- Test 5: BBR batch query (replicates pipeline pattern) ---
    batch_ok = False
    if geodkv_nodes:
        bbruuids = [n["BBRUUID"] for n in geodkv_nodes if n.get("BBRUUID")]
        if bbruuids:
            batch_results = test_bbr_batch_query(bbruuids)
            batch_ok = len(batch_results) > 0

    # --- Summary ---
    section("SUMMARY")
    geodkv_geom_ok = any(n.get("geometri", {}).get("wkt") for n in geodkv_nodes)
    bbr_coord_ok = (
        any(n.get("byg404Koordinat", {}).get("wkt") for n in bbr_nodes) if bbr_nodes else False
    )

    print(f"  GEODKV buildings:      {len(geodkv_nodes)} returned")
    print(
        f"  GEODKV geometry (WKT): {'YES' if geodkv_geom_ok else 'NO'} (SpatialPolygonZEpsg25832Type)"
    )
    print(f"  GEODKV spatial filter: {len(spatial_nodes)} buildings in bbox")
    print(f"  BBR buildings:         {len(bbr_nodes)} returned")
    print(f"  BBR coordinates (WKT): {'YES' if bbr_coord_ok else 'NO'} (SpatialPointEpsg25832Type)")
    print(f"  Cross-reference:       {'YES' if cross_ref_ok else 'NO'} (BBRUUID == id_lokalId)")
    print(f"  BBR batch query:       {'YES' if batch_ok else 'NO'}")

    all_ok = geodkv_geom_ok and bbr_coord_ok and cross_ref_ok
    print()
    if all_ok:
        print("  VERDICT: Both GEODKV and BBR GraphQL APIs work end-to-end.")
        print("  Ready to replace the broken WFS with GraphQL fetcher.")
    elif geodkv_geom_ok:
        print("  VERDICT: GEODKV works. BBR partially working.")
    elif geodkv_nodes or bbr_nodes:
        print("  VERDICT: Partial success. Some queries work, check errors above.")
    else:
        print("  VERDICT: No data returned. Check API key + IP whitelisting.")


if __name__ == "__main__":
    main()
