"""Silver stage: parse all bronze detail HTML files into a single Parquet table."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import duckdb
import pandas as pd

from .parse import parse_detail_html

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: tuple[str, ...] = (
    "kontrol_id",
    "link",
    "slug",
    "published_at",
    "modified_at",
    "sagsnummer",
    "kontrol_dato",
    "dyreart",
    "antal_dyr",
    "aarsag",
    "by",
    "chr_nummer",
    "cvr_nummer",
    "sanktion",
    "sanktion_ordinal",
    "kontroltekst",
    "tag_year",
    "tag_kommune",
    "tag_dyreart",
    "parsed_at",
)


def _build_slug_to_name(taxonomy: list[dict]) -> dict[str, str]:
    """Build a `kontrol_tag` slug → display name lookup."""
    return {
        item["slug"]: item["name"] for item in taxonomy if item.get("slug") and item.get("name")
    }


def transform_bronze_to_dataframe(bronze_dir: Path) -> pd.DataFrame:
    """Parse every detail HTML in the bronze run directory into a DataFrame."""
    bronze_dir = Path(bronze_dir)
    index_path = bronze_dir / "index.json"
    tag_path = bronze_dir / "kontrol_tag.json"
    details_dir = bronze_dir / "details"
    if not index_path.exists():
        raise FileNotFoundError(f"Missing bronze index: {index_path}")
    if not details_dir.exists():
        raise FileNotFoundError(f"Missing bronze details directory: {details_dir}")

    index = json.loads(index_path.read_text(encoding="utf-8"))
    taxonomy = json.loads(tag_path.read_text(encoding="utf-8")) if tag_path.exists() else []
    slug_to_name = _build_slug_to_name(taxonomy)
    index_by_id = {rec["id"]: rec for rec in index}

    rows: list[dict] = []
    parse_errors = 0
    for html_path in sorted(details_dir.glob("*.html")):
        try:
            wp_id = int(html_path.stem)
        except ValueError:
            logger.warning("Skipping non-numeric detail filename: %s", html_path.name)
            continue
        try:
            record = parse_detail_html(html_path.read_text(encoding="utf-8"), slug_to_name)
        except Exception as exc:
            logger.warning("Parse error for %s: %s", html_path, exc)
            parse_errors += 1
            continue

        # Override kontrol_id with the filename id (more reliable than HTML extraction).
        if record.get("kontrol_id") is None:
            record["kontrol_id"] = wp_id

        idx = index_by_id.get(wp_id, {})
        record["slug"] = idx.get("slug")
        record["published_at"] = idx.get("date")
        record["modified_at"] = idx.get("modified")
        if not record.get("link"):
            record["link"] = idx.get("link")
        rows.append(record)

    logger.info("Parsed %s records (%s errors)", len(rows), parse_errors)
    df = pd.DataFrame(rows)

    # Keep a stable schema even when there are zero rows (or partially populated rows).
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[list(REQUIRED_COLUMNS)]

    if "antal_dyr" in df.columns:
        df["antal_dyr"] = pd.to_numeric(df["antal_dyr"], errors="coerce").astype("Int64")
    return df


_TYPED_SELECT = """
SELECT
  CAST(kontrol_id AS BIGINT) AS kontrol_id,
  link,
  slug,
  CAST(published_at AS TIMESTAMP) AS published_at,
  CAST(modified_at AS TIMESTAMP) AS modified_at,
  sagsnummer,
  TRY_CAST(kontrol_dato AS DATE) AS kontrol_dato,
  dyreart,
  CAST(antal_dyr AS BIGINT) AS antal_dyr,
  aarsag,
  "by",
  chr_nummer,
  cvr_nummer,
  sanktion,
  CAST(sanktion_ordinal AS INTEGER) AS sanktion_ordinal,
  kontroltekst,
  tag_year,
  tag_kommune,
  tag_dyreart,
  CAST(parsed_at AS TIMESTAMP) AS parsed_at
FROM silver_df
"""


def write_parquet(df: pd.DataFrame, output_path: Path) -> Path:
    """Write a DataFrame to Parquet using DuckDB (zstd, single file, typed schema)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect()
    try:
        conn.register("silver_df", df)
        conn.execute(
            f"COPY ({_TYPED_SELECT}) TO '{output_path}' (FORMAT 'parquet', COMPRESSION 'zstd')"
        )
    finally:
        conn.close()
    return output_path


def run_silver(bronze_dir: Path, silver_dir: Path) -> dict:
    """Read bronze, parse, write parquet, return a summary dict."""
    df = transform_bronze_to_dataframe(bronze_dir)
    silver_dir = Path(silver_dir)
    parquet_path = silver_dir / "dyrenesdetektiv_kontrol.parquet"
    write_parquet(df, parquet_path)

    valid_chr = int(df["chr_nummer"].notna().sum()) if "chr_nummer" in df.columns else 0
    valid_cvr = int(df["cvr_nummer"].notna().sum()) if "cvr_nummer" in df.columns else 0
    summary = {
        "record_count": len(df),
        "valid_chr_count": valid_chr,
        "valid_cvr_count": valid_cvr,
        "parquet_path": str(parquet_path),
        "parquet_size_bytes": parquet_path.stat().st_size,
    }
    (silver_dir / "metadata.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary
