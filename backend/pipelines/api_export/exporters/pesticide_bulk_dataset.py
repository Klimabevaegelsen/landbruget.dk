"""Bulk pesticide dataset exporter.

This exporter publishes a documented research dataset, separate from the JSON API.
The public output intentionally excludes CVR numbers and describes rows as
field-level allocations of cumulative reported product use, not spray events.
"""

import hashlib
import json
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from common.logging_utils import get_pipeline_logger

from exporters.base import BaseExporter

logger = get_pipeline_logger("api_export.pesticide_bulk_dataset")


DATASET_PREFIX = "datasets/pesticide-field-use-allocations/v1"
DATASET_TITLE = "Landbruget.dk historical pesticide field-use allocations for Denmark"
SJI_GUIDANCE_URL = "https://www2.mst.dk/Udgiv/publikationer/2025/12/978-87-7564-073-7.pdf"


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


class PesticideBulkDatasetExporter(BaseExporter):
    """Export pesticide field-use allocations as Parquet plus metadata."""

    def export(self) -> dict:
        stats = {
            "files_written": 0,
            "allocation_years": [],
            "field_years": [],
            "products_written": False,
        }

        year_paths = self._discover_disaggregation_years()
        if not year_paths:
            logger.warning("No pesticide disaggregation data found")
            return stats

        checksums: dict[str, str] = {}
        quality_rows = []
        generated_at = datetime.now(UTC).isoformat()

        logger.info(f"Exporting pesticide bulk dataset for years: {sorted(year_paths)}")

        for year, parquet_path in sorted(year_paths.items()):
            self.load_parquet_table(parquet_path, "pesticide_bulk_source")
            self._create_public_use_allocations_table(year)
            allocation_path = f"{DATASET_PREFIX}/use_allocations/year={year}/part-000.parquet"
            checksums[allocation_path] = self._write_table_parquet(
                "public_use_allocations", allocation_path
            )
            stats["files_written"] += 1
            stats["allocation_years"].append(year)

            coverage = self._quality_metrics_for_year(year, parquet_path, "pesticide_bulk_source")
            quality_rows.append(coverage)
            coverage_path = f"{DATASET_PREFIX}/quality/year={year}/coverage.json"
            checksums[coverage_path] = self._write_text(
                json.dumps(self.to_json_compatible(coverage), indent=2, sort_keys=True) + "\n",
                coverage_path,
            )
            stats["files_written"] += 1

            field_path = self._latest_marker_path_for_field_year(year + 1)
            if field_path:
                self.load_parquet_table(field_path, "pesticide_bulk_fields_source")
                self._create_public_fields_table(year)
                fields_output_path = f"{DATASET_PREFIX}/fields/year={year}/part-000.parquet"
                checksums[fields_output_path] = self._write_table_parquet(
                    "public_fields", fields_output_path
                )
                stats["files_written"] += 1
                stats["field_years"].append(year)
            else:
                logger.warning(f"No FVM marker field source found for pesticide year {year}")

            self.conn.execute("DROP TABLE IF EXISTS pesticide_bulk_source")
            self.conn.execute("DROP TABLE IF EXISTS public_use_allocations")
            self.conn.execute("DROP TABLE IF EXISTS pesticide_bulk_fields_source")
            self.conn.execute("DROP TABLE IF EXISTS public_fields")

        checksums[f"{DATASET_PREFIX}/quality/all_years.csv"] = self._write_quality_summary(
            quality_rows
        )
        stats["files_written"] += 1

        products_path = self._latest_products_path()
        if products_path:
            self.load_parquet_table(products_path, "pesticide_bulk_products_source")
            self._create_public_products_table()
            products_output_path = f"{DATASET_PREFIX}/products/products.parquet"
            checksums[products_output_path] = self._write_table_parquet(
                "public_products", products_output_path
            )
            stats["files_written"] += 1
            stats["products_written"] = True
            self.conn.execute("DROP TABLE IF EXISTS pesticide_bulk_products_source")
        else:
            logger.warning("No BMD product metadata found; products resource skipped")

        metadata_files = {
            f"{DATASET_PREFIX}/README.md": self._readme(generated_at),
            f"{DATASET_PREFIX}/datapackage.json": json.dumps(
                self._datapackage(generated_at, stats), indent=2, sort_keys=True
            )
            + "\n",
            f"{DATASET_PREFIX}/examples/duckdb.sql": self._duckdb_example(),
            f"{DATASET_PREFIX}/dcat-ap.ttl": self._dcat_ap(generated_at),
        }
        for path, content in metadata_files.items():
            checksums[path] = self._write_text(content, path)
            stats["files_written"] += 1

        checksums_path = f"{DATASET_PREFIX}/checksums.txt"
        checksums_text = "".join(
            f"{digest}  {path.removeprefix(DATASET_PREFIX + '/')}\n"
            for path, digest in sorted(checksums.items())
        )
        self._write_text(checksums_text, checksums_path)
        stats["files_written"] += 1

        return stats

    def _discover_disaggregation_years(self) -> dict[int, str]:
        pattern = f"{self._r2_bucket}/gold/pesticide_disaggregation_*/*/*.parquet"
        try:
            files = self.r2_fs.glob(pattern)
        except Exception:
            logger.exception("Failed to list pesticide disaggregation files")
            return {}

        year_paths: dict[int, str] = {}
        for file_path in files:
            match = re.search(r"pesticide_disaggregation_(\d{4})_(\d{4})", file_path)
            if not match:
                continue
            year = int(match.group(1))
            if year not in year_paths or file_path > year_paths[year]:
                year_paths[year] = f"r2://{file_path}"
        return year_paths

    def _latest_marker_path_for_field_year(self, field_year: int) -> str | None:
        return self.latest_r2_parquet(f"silver/fvm_marker_{field_year}")

    def _latest_products_path(self) -> str | None:
        candidates = [
            "silver/bmd/*/pesticide_products.parquet",
            "silver/bmd/*/data.parquet",
            "gold/bmd/*/pesticide_products.parquet",
            "gold/bmd/*/data.parquet",
        ]
        for pattern in candidates:
            match = self.latest_r2_match(pattern)
            if match:
                return match
        return None

    def _create_public_use_allocations_table(self, year: int) -> None:
        columns = self._table_columns("pesticide_bulk_source")

        source_id_expr = self._source_id_expr(columns)
        cvr_expr = self._coalesce_expr(columns, ["cvr_number"], "NULL")
        crop_expr = self._coalesce_expr(columns, ["crop_code", "Code"], "NULL")
        reported_area_expr = self._coalesce_expr(
            columns,
            ["AcreageSize", "reported_cumulative_treated_area_ha", "area_ha"],
            "NULL",
            cast="DOUBLE",
        )
        allocated_area_expr = self._coalesce_expr(
            columns,
            ["AllocatedArea", "allocated_cumulative_treated_area_ha"],
            "NULL",
            cast="DOUBLE",
        )
        allocated_quantity_expr = self._coalesce_expr(
            columns,
            ["DosageQuantity", "allocated_quantity", "dosage_quantity"],
            "NULL",
            cast="DOUBLE",
        )
        unit_expr = self._coalesce_expr(
            columns,
            ["DosageUnit", "allocated_quantity_unit", "dosage_unit"],
            "NULL",
        )

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE public_use_allocations AS
            SELECT
                {year}::INTEGER AS year,
                DATE '{year}-08-01' AS plan_period_start,
                DATE '{year + 1}-07-31' AS plan_period_end,
                CAST({self._coalesce_expr(columns, ["field_uuid", "primary_field_id"], "NULL")} AS VARCHAR)
                    AS field_uuid,
                CAST({crop_expr} AS VARCHAR) AS crop_code,
                CAST({self._coalesce_expr(columns, ["PesticideName", "pesticide_name"], "NULL")} AS VARCHAR)
                    AS pesticide_name,
                CAST({self._coalesce_expr(columns, ["PesticideRegistrationNumber", "pesticide_registration_number"], "NULL")} AS VARCHAR)
                    AS pesticide_registration_number,
                CAST({reported_area_expr} AS DOUBLE) AS reported_cumulative_treated_area_ha,
                CAST({allocated_quantity_expr} AS DOUBLE) AS allocated_quantity,
                CAST({unit_expr} AS VARCHAR) AS allocated_quantity_unit,
                CAST({allocated_area_expr} AS DOUBLE) AS allocated_cumulative_treated_area_ha,
                CASE
                    WHEN {allocated_area_expr} IS NOT NULL AND {allocated_area_expr} > 0
                    THEN CAST({allocated_quantity_expr} AS DOUBLE) / CAST({allocated_area_expr} AS DOUBLE)
                    ELSE NULL
                END AS average_reported_rate_per_treated_ha,
                CAST({self._coalesce_expr(columns, ["AllocationMethod", "allocation_method"], "NULL")} AS VARCHAR)
                    AS allocation_method,
                CAST({self._coalesce_expr(columns, ["MatchConfidence", "match_confidence"], "NULL", cast="DOUBLE")} AS DOUBLE)
                    AS match_confidence,
                CAST({self._coalesce_expr(columns, ["IsPartialFieldCoverage", "is_partial_field_coverage"], "NULL", cast="BOOLEAN")} AS BOOLEAN)
                    AS is_partial_field_coverage,
                COUNT(*) OVER (
                    PARTITION BY
                        CAST({source_id_expr} AS VARCHAR),
                        CAST({cvr_expr} AS VARCHAR),
                        CAST({crop_expr} AS VARCHAR),
                        CAST({self._coalesce_expr(columns, ["PesticideRegistrationNumber", "pesticide_registration_number"], "NULL")} AS VARCHAR)
                )::INTEGER AS matched_field_count,
                CAST({self._coalesce_expr(columns, ["municipality"], "NULL")} AS VARCHAR)
                    AS municipality,
                sha256(CONCAT_WS('|', 'source', '{year}', CAST({source_id_expr} AS VARCHAR)))
                    AS source_record_hash,
                sha256(CONCAT_WS('|', 'holding-crop-period', '{year}', CAST({cvr_expr} AS VARCHAR), CAST({crop_expr} AS VARCHAR)))
                    AS holding_crop_period_hash,
                TIMESTAMP '{datetime.now(UTC).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")}'
                    AS generated_at
            FROM pesticide_bulk_source
        """)

    def _create_public_fields_table(self, pesticide_year: int) -> None:
        columns = self._table_columns("pesticide_bulk_fields_source")
        geometry_expr = "geometry" if "geometry" in columns else "NULL"
        field_uuid_expr = self._coalesce_expr(columns, ["field_uuid"], "NULL")
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE public_fields AS
            SELECT
                {pesticide_year}::INTEGER AS year,
                CAST({field_uuid_expr} AS VARCHAR) AS field_uuid,
                CAST({self._coalesce_expr(columns, ["crop_code"], "NULL")} AS VARCHAR) AS crop_code,
                CAST({self._coalesce_expr(columns, ["area_ha"], "NULL", cast="DOUBLE")} AS DOUBLE)
                    AS area_ha,
                CAST({self._coalesce_expr(columns, ["organic_farming", "is_organic"], "NULL", cast="BOOLEAN")} AS BOOLEAN)
                    AS organic_farming,
                CAST({self._coalesce_expr(columns, ["municipality"], "NULL")} AS VARCHAR)
                    AS municipality,
                {geometry_expr} AS geometry,
                'EPSG:25832' AS crs
            FROM pesticide_bulk_fields_source
            WHERE {field_uuid_expr} IS NOT NULL
              AND CAST({field_uuid_expr} AS VARCHAR) IN (
                  SELECT DISTINCT field_uuid
                  FROM public_use_allocations
                  WHERE field_uuid IS NOT NULL
              )
        """)

    def _create_public_products_table(self) -> None:
        columns = self._table_columns("pesticide_bulk_products_source")
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE public_products AS
            SELECT DISTINCT
                CAST({self._coalesce_expr(columns, ["registrerings_nr", "registration_number", "PesticideRegistrationNumber"], "NULL")} AS VARCHAR)
                    AS pesticide_registration_number,
                CAST({self._coalesce_expr(columns, ["product_name", "produktnavn", "PesticideName"], "NULL")} AS VARCHAR)
                    AS product_name,
                CAST({self._coalesce_expr(columns, ["active_substances", "aktivstoffer"], "NULL")} AS VARCHAR)
                    AS active_substances,
                CAST({self._coalesce_expr(columns, ["approval_status", "godkendelsesstatus"], "NULL")} AS VARCHAR)
                    AS approval_status,
                CAST({self._coalesce_expr(columns, ["pfas_flag", "contains_pfas"], "NULL", cast="BOOLEAN")} AS BOOLEAN)
                    AS pfas_flag,
                CAST({self._coalesce_expr(columns, ["samlet_belastning"], "NULL", cast="DOUBLE")} AS DOUBLE)
                    AS samlet_belastning
            FROM pesticide_bulk_products_source
        """)

    def _quality_metrics_for_year(self, year: int, source_path: str, table_name: str) -> dict:
        rows = self.query_to_dicts(f"""
            SELECT
                {year}::INTEGER AS year,
                COUNT(*)::BIGINT AS public_allocation_rows,
                COUNT(DISTINCT field_uuid)::BIGINT AS unique_fields,
                COUNT(DISTINCT source_record_hash)::BIGINT AS unique_source_records,
                ROUND(SUM(COALESCE(allocated_quantity, 0)), 6) AS total_allocated_quantity,
                ROUND(SUM(COALESCE(allocated_cumulative_treated_area_ha, 0)), 6)
                    AS total_allocated_cumulative_treated_area_ha,
                ROUND(AVG(match_confidence), 6) AS average_match_confidence,
                COUNT(*) FILTER (WHERE is_partial_field_coverage)::BIGINT AS partial_field_rows
            FROM public_use_allocations
        """)

        allocation_methods = self.query_to_dicts("""
            SELECT
                allocation_method,
                COUNT(*)::BIGINT AS rows
            FROM public_use_allocations
            GROUP BY allocation_method
            ORDER BY rows DESC, allocation_method
        """)

        source_total = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        metrics = rows[0] if rows else {"year": year}
        metrics.update(
            {
                "source_path": source_path,
                "source_rows": int(source_total),
                "allocation_methods": allocation_methods,
                "interpretation": (
                    "Rows are field-level allocations of cumulative reported product-use totals. "
                    "They are not individual spray events and do not encode application dates, "
                    "pass counts, or per-pass dose."
                ),
            }
        )
        return metrics

    def _write_quality_summary(self, quality_rows: list[dict]) -> str:
        values = ", ".join(
            [
                "("
                + ", ".join(
                    [
                        str(int(row.get("year") or 0)),
                        str(int(row.get("public_allocation_rows") or 0)),
                        str(int(row.get("unique_fields") or 0)),
                        str(int(row.get("unique_source_records") or 0)),
                        str(float(row.get("total_allocated_quantity") or 0)),
                        str(float(row.get("total_allocated_cumulative_treated_area_ha") or 0)),
                        str(float(row.get("average_match_confidence") or 0)),
                        str(int(row.get("partial_field_rows") or 0)),
                        str(int(row.get("source_rows") or 0)),
                    ]
                )
                + ")"
                for row in quality_rows
            ]
        )
        if not values:
            values = "(0, 0, 0, 0, 0, 0, 0, 0, 0)"
        quality_sql = f"""
            SELECT *
            FROM (VALUES {values}) AS quality_rows(
                year,
                public_allocation_rows,
                unique_fields,
                unique_source_records,
                total_allocated_quantity,
                total_allocated_cumulative_treated_area_ha,
                average_match_confidence,
                partial_field_rows,
                source_rows
            )
            WHERE year != 0
            ORDER BY year
        """
        if self.output_dir:
            path = self._output_path(f"{DATASET_PREFIX}/quality/all_years.csv")
            path.parent.mkdir(parents=True, exist_ok=True)
            self.conn.execute(f"""
                COPY ({quality_sql})
                TO '{path}' (HEADER, DELIMITER ',')
            """)
            return self._sha256_file(path)

        digest = ""
        with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
            self.conn.execute(f"""
                COPY ({quality_sql})
                TO '{tmp.name}' (HEADER, DELIMITER ',')
            """)
            digest = self._sha256_file(Path(tmp.name))
            self._upload_file(tmp.name, f"{DATASET_PREFIX}/quality/all_years.csv")
        return digest

    def _table_columns(self, table_name: str) -> set[str]:
        return {
            row[1] for row in self.conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        }

    def _coalesce_expr(
        self,
        columns: set[str],
        candidates: list[str],
        default: str,
        *,
        cast: str | None = None,
    ) -> str:
        expressions = []
        for candidate in candidates:
            if candidate in columns:
                expr = _quote_identifier(candidate)
                if cast:
                    expr = f"TRY_CAST({expr} AS {cast})"
                expressions.append(expr)
        expressions.append(default)
        if len(expressions) == 1:
            return expressions[0]
        return f"COALESCE({', '.join(expressions)})"

    def _source_id_expr(self, columns: set[str]) -> str:
        return self._coalesce_expr(
            columns,
            ["OriginalPesticideRowID", "source_record_id", "DisaggregatedID"],
            "CAST(rowid AS VARCHAR)",
            cast="VARCHAR",
        )

    def _write_table_parquet(self, table_name: str, relative_path: str) -> str:
        if self.output_dir:
            output_path = self._output_path(relative_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            self.conn.execute(
                f"COPY {table_name} TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            digest = self._sha256_file(output_path)
            logger.info(f"Wrote {output_path}")
            return digest

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            self.conn.execute(
                f"COPY {table_name} TO '{tmp.name}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            digest = self._sha256_file(Path(tmp.name))
            self._upload_file(tmp.name, relative_path)
            return digest

    def _write_text(self, content: str, relative_path: str) -> str:
        data = content.encode("utf-8")
        if self.output_dir:
            output_path = self._output_path(relative_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(data)
            logger.info(f"Wrote {output_path}")
        else:
            self._upload_bytes(data, relative_path)
        return hashlib.sha256(data).hexdigest()

    def _output_path(self, relative_path: str) -> Path:
        if not self.output_dir:
            raise ValueError("Local output path requested without output_dir")
        return Path(self.output_dir) / relative_path

    def _upload_file(self, local_path: str, relative_path: str) -> None:
        r2_path = f"{self._r2_bucket}/{relative_path}"
        with open(local_path, "rb") as src, self.r2_fs.open(r2_path, "wb") as dst:
            dst.write(src.read())
        logger.info(f"Uploaded s3://{r2_path}")

    def _upload_bytes(self, data: bytes, relative_path: str) -> None:
        r2_path = f"{self._r2_bucket}/{relative_path}"
        with self.r2_fs.open(r2_path, "wb") as dst:
            dst.write(data)
        logger.info(f"Uploaded s3://{r2_path}")

    def _sha256_file(self, path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _readme(self, generated_at: str) -> str:
        return f"""# {DATASET_TITLE}

Generated: {generated_at}

This dataset publishes field-level allocations of cumulative reported pesticide product use for
Denmark. Rows are not individual spray events. The source SJI reports are summarized by reporting
unit, crop, product, quantity, and cumulative treated area for an agricultural planning period.

Important interpretation limits:

- No row contains an application date.
- No row contains the number of times or passes a product was applied.
- `average_reported_rate_per_treated_ha` is total allocated quantity divided by cumulative
  allocated treated area, not a per-pass dose.
- Field assignment is derived from CVR/crop/area matching in the internal method, but public files
  exclude raw CVR numbers.
- Field-level amounts are allocations, not observed field measurements.

Resources:

- `use_allocations/year=YYYY/part-000.parquet`: public field-use allocation rows.
- `fields/year=YYYY/part-000.parquet`: public field geometries, without CVR.
- `products/products.parquet`: pesticide product metadata when available.
- `quality/`: row counts, coverage metadata, checksums, and method caveats.

Source and method references:

- SJI spray-journal guidance: {SJI_GUIDANCE_URL}
- FVM field geodata: https://lbst.dk/bedrift/arealer-og-ejendomme/kortdata/adgang-til-kortdata
- Repository: https://github.com/Klimabevaegelsen/landbruget.dk
"""

    def _datapackage(self, generated_at: str, stats: dict) -> dict:
        return {
            "profile": "data-package",
            "name": "pesticide-field-use-allocations",
            "title": DATASET_TITLE,
            "version": "1.0.0",
            "created": generated_at,
            "homepage": "https://landbruget.dk",
            "description": (
                "Field-level allocations of cumulative reported pesticide product use. "
                "Rows are not individual spray events and public files exclude raw CVR numbers."
            ),
            "keywords": [
                "Denmark",
                "pesticides",
                "agriculture",
                "geospatial",
                "field boundaries",
                "spray journal",
                "environmental exposure",
            ],
            "sources": [
                {"title": "Miljoestyrelsen SJI spray-journal guidance", "path": SJI_GUIDANCE_URL},
                {
                    "title": "Landbrugsstyrelsen field geodata",
                    "path": "https://lbst.dk/bedrift/arealer-og-ejendomme/kortdata/adgang-til-kortdata",
                },
            ],
            "licenses": [
                {
                    "name": "custom",
                    "title": "Reuse terms must be confirmed against upstream SJI, FVM, and BMD terms",
                }
            ],
            "resources": [
                {
                    "name": "use_allocations",
                    "path": "use_allocations/year=*/part-000.parquet",
                    "format": "parquet",
                    "description": "Field-level allocations of cumulative reported pesticide use.",
                },
                {
                    "name": "fields",
                    "path": "fields/year=*/part-000.parquet",
                    "format": "parquet",
                    "description": "Field geometries and crop metadata without CVR numbers.",
                },
                {
                    "name": "products",
                    "path": "products/products.parquet",
                    "format": "parquet",
                    "description": "Product metadata when available.",
                },
                {
                    "name": "quality",
                    "path": "quality/all_years.csv",
                    "format": "csv",
                    "description": "Per-year publication quality metrics.",
                },
            ],
            "publication_stats": stats,
        }

    def _duckdb_example(self) -> str:
        return f"""-- Query public pesticide field-use allocations directly with DuckDB.
SELECT
  year,
  pesticide_name,
  SUM(allocated_quantity) AS total_allocated_quantity,
  SUM(allocated_cumulative_treated_area_ha) AS cumulative_treated_area_ha
FROM read_parquet(
  'https://data.landbruget.dk/{DATASET_PREFIX}/use_allocations/year=*/part-000.parquet',
  hive_partitioning = true
)
GROUP BY year, pesticide_name
ORDER BY year, total_allocated_quantity DESC;
"""

    def _dcat_ap(self, generated_at: str) -> str:
        return f"""@prefix dcat: <http://www.w3.org/ns/dcat#> .
@prefix dct: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://landbruget.dk/datasets/pesticide-field-use-allocations/v1>
  a dcat:Dataset ;
  dct:title "{DATASET_TITLE}" ;
  dct:issued "{generated_at}"^^xsd:dateTime ;
  dct:description "Field-level allocations of cumulative reported pesticide product use. Rows are not individual spray events and public files exclude raw CVR numbers." ;
  dcat:keyword "Denmark", "pesticides", "agriculture", "geospatial", "field boundaries" ;
  dcat:distribution <https://data.landbruget.dk/{DATASET_PREFIX}/datapackage.json> .
"""
