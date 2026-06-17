import json

import pytest

from unified_pipeline.silver.skovrejsning import SkovrejsningSilver, SkovrejsningSilverConfig


@pytest.fixture
def silver_config() -> SkovrejsningSilverConfig:
    return SkovrejsningSilverConfig()


@pytest.fixture
def skovrejsning_silver(silver_config: SkovrejsningSilverConfig) -> SkovrejsningSilver:
    return SkovrejsningSilver(silver_config)


@pytest.fixture
def sample_geojson_payload() -> str:
    def polygon(x: float, y: float) -> dict:
        return {
            "type": "Polygon",
            "coordinates": [
                [
                    [x, y],
                    [x + 1000, y],
                    [x + 1000, y + 1000],
                    [x, y + 1000],
                    [x, y],
                ]
            ],
        }

    return json.dumps(
        {
            "type": "FeatureCollection",
            "numberMatched": 3,
            "numberReturned": 3,
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "omraadenavn": "Uønsket",
                        "komnavn": "Aarhus",
                        "plangrund": "Vedtaget",
                        "plannr": "1",
                        "temanavn": "Skovrejsningsområde",
                        "datovedt": "2024-01-01T00:00:00",
                        "uuid": "undesired-uuid",
                        "objekt_id": "1",
                    },
                    "geometry": polygon(560000, 6220000),
                },
                {
                    "type": "Feature",
                    "properties": {
                        "omraadenavn": "Ønsket",
                        "komnavn": "Aarhus",
                        "plangrund": "Vedtaget",
                        "plannr": "2",
                        "temanavn": "Skovrejsningsområde",
                        "datovedt": "2024-01-02T00:00:00",
                        "uuid": "desired-uuid",
                        "objekt_id": "2",
                    },
                    "geometry": polygon(562000, 6220000),
                },
                {
                    "type": "Feature",
                    "properties": {
                        "omraadenavn": "Neutral",
                        "komnavn": "Aarhus",
                        "plangrund": "Vedtaget",
                        "plannr": "3",
                        "temanavn": "Skovrejsningsområde",
                        "datovedt": "2024-01-03T00:00:00",
                        "uuid": "neutral-uuid",
                        "objekt_id": "3",
                    },
                    "geometry": polygon(564000, 6220000),
                },
            ],
        }
    )


def test_skovrejsning_silver_config(silver_config: SkovrejsningSilverConfig) -> None:
    assert silver_config.dataset == "skovrejsning"
    assert silver_config.bucket == "landbruget-data"
    assert silver_config.storage_batch_size == 5000
    assert silver_config.category_mapping["Uønsket"] == "undesired"
    assert silver_config.category_mapping["Ønsket"] == "desired"


def test_read_bronze_data_with_in_memory_data(
    skovrejsning_silver: SkovrejsningSilver,
    silver_config: SkovrejsningSilverConfig,
    sample_geojson_payload: str,
) -> None:
    result = skovrejsning_silver._read_bronze_data(
        silver_config.dataset, silver_config.bucket, bronze_data=[sample_geojson_payload]
    )

    assert result is not None
    assert isinstance(result, str)

    row_count = skovrejsning_silver.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count == 1

    payload = skovrejsning_silver.conn.execute(f"SELECT payload FROM {result}").fetchone()[0]
    assert payload == sample_geojson_payload


def test_process_geojson_data_maps_categories_and_geometry(
    skovrejsning_silver: SkovrejsningSilver,
    sample_geojson_payload: str,
) -> None:
    result = skovrejsning_silver._process_geojson_data({"payload": [sample_geojson_payload]})

    assert result is not None
    assert isinstance(result, str)

    row_count = skovrejsning_silver.conn.execute(f"SELECT COUNT(*) FROM {result}").fetchone()[0]
    assert row_count == 3

    rows = skovrejsning_silver.conn.execute(f"""
        SELECT omraadenavn, category, area_ha, ST_IsValid(geometry_spatial), geometry
        FROM {result}
        ORDER BY objekt_id
    """).fetchall()

    assert [(row[0], row[1]) for row in rows] == [
        ("Uønsket", "undesired"),
        ("Ønsket", "desired"),
        ("Neutral", "neutral"),
    ]
    assert all(row[2] and row[2] > 0 for row in rows)
    assert all(row[3] for row in rows)
    assert all(row[4] for row in rows)


def test_process_geojson_data_with_empty_payload(skovrejsning_silver: SkovrejsningSilver) -> None:
    result = skovrejsning_silver._process_geojson_data(
        {"payload": [json.dumps({"type": "FeatureCollection", "features": []})]}
    )

    assert result is None


def test_create_dissolved_df_groups_by_category(
    skovrejsning_silver: SkovrejsningSilver,
    sample_geojson_payload: str,
) -> None:
    processed_table = skovrejsning_silver._process_geojson_data(
        {"payload": [sample_geojson_payload]}
    )
    assert processed_table is not None

    dissolved_table = skovrejsning_silver._create_dissolved_df(
        processed_table, skovrejsning_silver.config.dataset
    )

    categories = skovrejsning_silver.conn.execute(f"""
        SELECT category, ST_IsEmpty(geometry) as is_empty
        FROM {dissolved_table}
        ORDER BY category
    """).fetchall()

    assert categories == [
        ("desired", False),
        ("neutral", False),
        ("undesired", False),
    ]
