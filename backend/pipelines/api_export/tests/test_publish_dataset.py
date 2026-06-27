import argparse
import hashlib
import json
import zipfile
from pathlib import Path

import duckdb
import pytest

from publish_dataset import (
    DatasetBundle,
    HttpClient,
    PublicationError,
    build_parser,
    dataverse_dataset_json,
    run,
    zenodo_metadata,
)


def _write_dataset(root: Path, *, include_cvr: bool = False) -> Path:
    dataset_root = root / "datasets" / "pesticide-field-use-allocations" / "v1"
    (dataset_root / "use_allocations" / "year=2024").mkdir(parents=True)
    (dataset_root / "fields" / "year=2024").mkdir(parents=True)
    (dataset_root / "products").mkdir()
    (dataset_root / "quality").mkdir()
    (dataset_root / "examples").mkdir()

    allocation_select = "SELECT 2024 AS year, 'field-1' AS field_uuid, 'product' AS pesticide_name"
    if include_cvr:
        allocation_select = "SELECT 2024 AS year, 'field-1' AS field_uuid, '12345678' AS cvr_number"
    duckdb.sql(
        f"""
        COPY ({allocation_select})
        TO '{dataset_root / "use_allocations" / "year=2024" / "part-000.parquet"}'
        (FORMAT PARQUET)
        """
    )
    duckdb.sql(
        f"""
        COPY (SELECT 2024 AS year, 'field-1' AS field_uuid, 'POINT (0 0)' AS geometry)
        TO '{dataset_root / "fields" / "year=2024" / "part-000.parquet"}'
        (FORMAT PARQUET)
        """
    )
    duckdb.sql(
        f"""
        COPY (SELECT '1' AS pesticide_registration_number, 'product' AS product_name)
        TO '{dataset_root / "products" / "products.parquet"}'
        (FORMAT PARQUET)
        """
    )

    (dataset_root / "README.md").write_text("README\n", encoding="utf-8")
    (dataset_root / "examples" / "duckdb.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (dataset_root / "quality" / "all_years.csv").write_text("year,rows\n2024,1\n", encoding="utf-8")
    (dataset_root / "datapackage.json").write_text(
        json.dumps(
            {
                "title": "Test pesticide use allocations",
                "description": "Rows are not individual spray events.",
                "keywords": ["pesticides", "Denmark"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    checksum_lines = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  "
        f"{path.relative_to(dataset_root).as_posix()}\n"
        for path in sorted(dataset_root.rglob("*"))
        if path.is_file() and path.name != "checksums.txt"
    ]
    (dataset_root / "checksums.txt").write_text("".join(checksum_lines), encoding="utf-8")
    return dataset_root


def _args(source_dir: Path, *targets: str, **overrides: object) -> argparse.Namespace:
    args = build_parser().parse_args(
        [
            "--source-dir",
            str(source_dir),
            "--target",
            targets[0] if targets else "zenodo",
        ]
    )
    for target in targets[1:]:
        args.target.append(target)
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_bundle_validation_and_archive_creation(tmp_path: Path) -> None:
    dataset_root = _write_dataset(tmp_path)
    bundle = DatasetBundle(dataset_root)

    assert bundle.validate() == []
    archive_path = bundle.create_archive()

    assert archive_path.exists()
    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
    assert "use_allocations/year=2024/part-000.parquet" in names
    assert "README.md" in names


def test_bundle_validation_rejects_cvr_columns(tmp_path: Path) -> None:
    dataset_root = _write_dataset(tmp_path, include_cvr=True)

    errors = DatasetBundle(dataset_root).validate()

    assert any("cvr_number" in error for error in errors)


def test_run_defaults_to_dry_run_without_confirm_publish(tmp_path: Path) -> None:
    dataset_root = _write_dataset(tmp_path)

    results = run(_args(dataset_root, "zenodo", "figshare", "dataverse"))

    assert [result.target for result in results] == ["zenodo", "figshare", "dataverse"]
    assert all(result.status == "dry-run" for result in results)


class RecordingClient(HttpClient):
    def __init__(self) -> None:
        self.requests: list[tuple[str, str, object | None]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_body: object | None = None,
        data: bytes | None = None,
    ) -> tuple[int, dict[str, str], object]:
        self.requests.append((method, url, json_body if json_body is not None else data))
        if method == "POST" and url.endswith("/api/deposit/depositions"):
            return (
                201,
                {},
                {"id": 123, "links": {"bucket": "https://zenodo.example/api/files/bucket"}},
            )
        if method == "PUT" and url.endswith("/api/deposit/depositions/123"):
            return 200, {}, {}
        if method == "PUT" and url.startswith("https://zenodo.example/api/files/bucket"):
            return 200, {}, {}
        if method == "POST" and url.endswith("/actions/publish"):
            return (
                202,
                {},
                {
                    "metadata": {"doi": "10.5281/zenodo.123"},
                    "links": {"html": "https://zenodo/123"},
                },
            )
        if method == "GET" and url.endswith("/api/deposit/depositions/123"):
            return (
                200,
                {},
                {
                    "metadata": {"prereserve_doi": {"doi": "10.5281/zenodo.123-draft"}},
                    "links": {"html": "https://zenodo/123/draft"},
                },
            )
        raise AssertionError(f"unexpected request: {method} {url}")


def test_zenodo_publish_sequence_with_confirm(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_root = _write_dataset(tmp_path)
    client = RecordingClient()
    monkeypatch.setenv("ZENODO_TOKEN", "token")

    args = _args(
        dataset_root,
        "zenodo",
        confirm_publish=True,
        license_id="cc-by-4.0",
        zenodo_sandbox=False,
    )

    results = run(args, client=client)

    assert results[0].status == "published"
    assert results[0].doi == "10.5281/zenodo.123"
    assert [request[0] for request in client.requests] == ["POST", "PUT", "PUT", "POST"]
    metadata_request = client.requests[1][2]
    assert isinstance(metadata_request, dict)
    assert metadata_request["metadata"]["license"] == "cc-by-4.0"


def test_non_dry_run_requires_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_root = _write_dataset(tmp_path)
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)
    args = _args(dataset_root, "zenodo", confirm_publish=True, license_id="cc-by-4.0")

    with pytest.raises(PublicationError, match="ZENODO_TOKEN"):
        run(args, client=RecordingClient())


def test_zenodo_create_draft_does_not_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_root = _write_dataset(tmp_path)
    client = RecordingClient()
    monkeypatch.setenv("ZENODO_TOKEN", "token")

    args = _args(
        dataset_root,
        "zenodo",
        create_draft=True,
        license_id="cc-by-4.0",
        zenodo_sandbox=False,
    )

    results = run(args, client=client)

    assert results[0].status == "draft"
    assert results[0].doi == "10.5281/zenodo.123-draft"
    assert [request[0] for request in client.requests] == ["POST", "PUT", "PUT", "GET"]


def test_metadata_helpers_include_platform_required_fields() -> None:
    args = _args(Path("/tmp/unused"), "zenodo", license_id="cc-by-4.0")
    metadata = DatasetBundle(Path("/tmp/unused")).load_metadata(args)

    zenodo = zenodo_metadata(metadata)
    dataverse = dataverse_dataset_json(metadata, "Agricultural Sciences")

    assert zenodo["upload_type"] == "dataset"
    citation_fields = dataverse["datasetVersion"]["metadataBlocks"]["citation"]["fields"]
    assert {field["typeName"] for field in citation_fields} >= {
        "title",
        "author",
        "datasetContact",
        "dsDescription",
        "subject",
    }
