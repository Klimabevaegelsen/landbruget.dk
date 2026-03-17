"""
Baseline management for data validation.

This module provides utilities to:
- Generate baseline metrics from production data
- Compare current outputs against baselines
- Store/retrieve baselines from GCS

Used in pre-merge validation to detect data accuracy regressions.
"""

import hashlib
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass
class BaselineMetrics:
    """Metrics captured from a dataset for baseline comparison."""

    dataset_name: str
    record_count: int
    total_area_m2: float | None = None
    unique_identifiers: dict[str, int] = field(default_factory=dict)
    geometry_bounds: dict[str, float] | None = None
    detected_crs: str | None = None
    column_null_counts: dict[str, int] = field(default_factory=dict)
    checksum: str | None = None
    generated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    source_commit: str | None = None
    source_path: str | None = None  # Path to the source data file

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BaselineMetrics":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class ComparisonResult:
    """Result of comparing current metrics against baseline."""

    dataset_name: str
    is_valid: bool
    checks: list[dict[str, Any]]
    baseline_metrics: BaselineMetrics
    current_metrics: BaselineMetrics
    tolerance_pct: float

    @property
    def failed_checks(self) -> list[dict[str, Any]]:
        """Return only failed checks."""
        return [c for c in self.checks if not c["passed"]]

    @property
    def summary(self) -> str:
        """Human-readable summary of comparison."""
        if self.is_valid:
            return f"✅ {self.dataset_name}: All {len(self.checks)} checks passed"
        failed = len(self.failed_checks)
        return f"❌ {self.dataset_name}: {failed}/{len(self.checks)} checks failed"


class BaselineManager:
    """Manages baseline generation, storage, and comparison."""

    def __init__(
        self,
        baseline_path: str | Path,
        tolerance_pct: float = 1.0,
        gcs_client: Any = None,
    ):
        """
        Initialize baseline manager.

        Args:
            baseline_path: Local path or GCS path (gs://...) to baselines
            tolerance_pct: Default tolerance for numerical comparisons
            gcs_client: Optional GCS client for cloud storage operations
        """
        self.baseline_path = (
            Path(baseline_path)
            if not str(baseline_path).startswith("gs://")
            else baseline_path
        )
        self.tolerance_pct = tolerance_pct
        self.gcs_client = gcs_client
        self._is_gcs = str(baseline_path).startswith("gs://")

    def generate_metrics_from_parquet(
        self,
        parquet_path: str | Path,
        dataset_name: str,
        area_column: str | None = "area_m2",
        identifier_columns: list[str] | None = None,
        geometry_column: str | None = "geometry",
    ) -> BaselineMetrics:
        """
        Generate baseline metrics from a parquet file.

        Args:
            parquet_path: Path to parquet file
            dataset_name: Name of the dataset
            area_column: Column containing area values (optional)
            identifier_columns: Columns containing identifiers (CVR, CHR, etc.)
            geometry_column: Column containing geometry (optional)

        Returns:
            BaselineMetrics with computed statistics
        """
        logger.info(
            f"Generating baseline metrics for {dataset_name} from {parquet_path}"
        )

        # Read parquet file
        table = pq.read_table(parquet_path)
        df = table.to_pandas()

        # Basic counts
        record_count = len(df)

        # Area statistics
        total_area = None
        if area_column and area_column in df.columns:
            total_area = float(df[area_column].sum())

        # Identifier counts
        unique_identifiers = {}
        if identifier_columns:
            for col in identifier_columns:
                if col in df.columns:
                    unique_identifiers[col] = int(df[col].nunique())

        # Column null counts
        column_null_counts = {col: int(df[col].isna().sum()) for col in df.columns}

        # Geometry bounds (if geometry column exists and is WKB/WKT)
        geometry_bounds = None
        detected_crs = None
        if geometry_column and geometry_column in df.columns:
            try:
                geometry_bounds, detected_crs = self._extract_geometry_bounds(
                    df, geometry_column
                )
            except Exception as e:
                logger.warning(f"Could not extract geometry bounds: {e}")

        # Compute checksum of first N rows for quick comparison
        checksum = self._compute_checksum(df.head(1000))

        return BaselineMetrics(
            dataset_name=dataset_name,
            record_count=record_count,
            total_area_m2=total_area,
            unique_identifiers=unique_identifiers,
            geometry_bounds=geometry_bounds,
            detected_crs=detected_crs,
            column_null_counts=column_null_counts,
            checksum=checksum,
        )

    def generate_metrics_from_duckdb(
        self,
        conn: Any,
        table_name: str,
        dataset_name: str,
        area_column: str | None = "area_m2",
        identifier_columns: list[str] | None = None,
        geometry_column: str | None = "geometry",
    ) -> BaselineMetrics:
        """
        Generate baseline metrics from a DuckDB table.

        Args:
            conn: DuckDB connection
            table_name: Name of table to analyze
            dataset_name: Name for the baseline
            area_column: Column containing area values
            identifier_columns: Columns containing identifiers
            geometry_column: Column containing geometry

        Returns:
            BaselineMetrics with computed statistics
        """
        logger.info(
            f"Generating baseline metrics for {dataset_name} from DuckDB table {table_name}"
        )

        # Record count
        record_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        # Area total
        total_area = None
        if area_column:
            result = conn.execute(f"""
                SELECT COALESCE(SUM({area_column}), 0)
                FROM {table_name}
                WHERE {area_column} IS NOT NULL
            """).fetchone()
            total_area = float(result[0]) if result else None

        # Identifier counts
        unique_identifiers = {}
        if identifier_columns:
            for col in identifier_columns:
                result = conn.execute(f"""
                    SELECT COUNT(DISTINCT {col}) FROM {table_name} WHERE {col} IS NOT NULL
                """).fetchone()
                unique_identifiers[col] = int(result[0]) if result else 0

        # Geometry bounds
        geometry_bounds = None
        detected_crs = None
        if geometry_column:
            try:
                result = conn.execute(f"""
                    SELECT
                        MIN(ST_XMin({geometry_column})) as min_x,
                        MAX(ST_XMax({geometry_column})) as max_x,
                        MIN(ST_YMin({geometry_column})) as min_y,
                        MAX(ST_YMax({geometry_column})) as max_y
                    FROM {table_name}
                    WHERE {geometry_column} IS NOT NULL
                """).fetchone()
                if result and result[0] is not None:
                    geometry_bounds = {
                        "min_x": float(result[0]),
                        "max_x": float(result[1]),
                        "min_y": float(result[2]),
                        "max_y": float(result[3]),
                    }
                    # Import CRS detection
                    from common.crs_utils import detect_crs_from_bounds

                    detected_crs, _ = detect_crs_from_bounds(
                        result[0], result[1], result[2], result[3]
                    )
            except Exception as e:
                logger.warning(f"Could not extract geometry bounds: {e}")

        # Column null counts
        columns = conn.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()
        column_null_counts = {}
        for (col,) in columns:
            result = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL
            """).fetchone()
            column_null_counts[col] = int(result[0]) if result else 0

        # Checksum from sample
        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 1000").fetchdf()
        checksum = self._compute_checksum(sample)

        return BaselineMetrics(
            dataset_name=dataset_name,
            record_count=record_count,
            total_area_m2=total_area,
            unique_identifiers=unique_identifiers,
            geometry_bounds=geometry_bounds,
            detected_crs=detected_crs,
            column_null_counts=column_null_counts,
            checksum=checksum,
        )

    def save_baseline(self, metrics: BaselineMetrics) -> str:
        """
        Save baseline metrics to storage.

        Args:
            metrics: BaselineMetrics to save

        Returns:
            Path where baseline was saved
        """
        if self._is_gcs:
            return self._save_to_gcs(metrics)
        return self._save_to_local(metrics)

    def load_baseline(self, dataset_name: str) -> BaselineMetrics | None:
        """
        Load baseline metrics from storage.

        Args:
            dataset_name: Name of dataset to load baseline for

        Returns:
            BaselineMetrics or None if not found
        """
        if self._is_gcs:
            return self._load_from_gcs(dataset_name)
        return self._load_from_local(dataset_name)

    def compare(
        self,
        current: BaselineMetrics,
        baseline: BaselineMetrics | None = None,
        tolerance_pct: float | None = None,
    ) -> ComparisonResult:
        """
        Compare current metrics against baseline.

        Args:
            current: Current metrics to validate
            baseline: Baseline to compare against (loads from storage if not provided)
            tolerance_pct: Override default tolerance

        Returns:
            ComparisonResult with pass/fail status and details
        """
        if baseline is None:
            baseline = self.load_baseline(current.dataset_name)
            if baseline is None:
                raise ValueError(f"No baseline found for {current.dataset_name}")

        tol = tolerance_pct if tolerance_pct is not None else self.tolerance_pct
        checks = []

        # Check 1: Record count
        checks.append(
            self._check_numeric(
                "record_count",
                baseline.record_count,
                current.record_count,
                tol,
            )
        )

        # Check 2: Total area (if available)
        if baseline.total_area_m2 is not None and current.total_area_m2 is not None:
            checks.append(
                self._check_numeric(
                    "total_area_m2",
                    baseline.total_area_m2,
                    current.total_area_m2,
                    tol,
                )
            )

        # Check 3: CRS consistency
        if baseline.detected_crs is not None:
            checks.append(
                {
                    "name": "crs_consistency",
                    "baseline": baseline.detected_crs,
                    "current": current.detected_crs,
                    "passed": baseline.detected_crs == current.detected_crs,
                    "message": (
                        f"CRS match: {current.detected_crs}"
                        if baseline.detected_crs == current.detected_crs
                        else f"CRS changed: {baseline.detected_crs} -> {current.detected_crs}"
                    ),
                }
            )

        # Check 4: Identifier counts
        for id_col, baseline_count in baseline.unique_identifiers.items():
            if id_col in current.unique_identifiers:
                checks.append(
                    self._check_numeric(
                        f"unique_{id_col}",
                        baseline_count,
                        current.unique_identifiers[id_col],
                        tol,
                    )
                )

        # Check 5: Geometry bounds (should be within Denmark)
        if current.geometry_bounds:
            checks.append(self._check_geometry_bounds(current.geometry_bounds))

        # Overall pass/fail
        is_valid = all(c["passed"] for c in checks)

        return ComparisonResult(
            dataset_name=current.dataset_name,
            is_valid=is_valid,
            checks=checks,
            baseline_metrics=baseline,
            current_metrics=current,
            tolerance_pct=tol,
        )

    def _check_numeric(
        self,
        name: str,
        baseline_value: float,
        current_value: float,
        tolerance_pct: float,
    ) -> dict[str, Any]:
        """Check if numeric value is within tolerance of baseline."""
        if baseline_value == 0:
            diff_pct = 0 if current_value == 0 else 100
        else:
            diff_pct = abs(current_value - baseline_value) / baseline_value * 100

        passed = diff_pct <= tolerance_pct

        return {
            "name": name,
            "baseline": baseline_value,
            "current": current_value,
            "difference_pct": diff_pct,
            "tolerance_pct": tolerance_pct,
            "passed": passed,
            "message": (
                f"{name}: {current_value:,.0f} (within {tolerance_pct}%)"
                if passed
                else f"{name}: {current_value:,.0f} ({diff_pct:.2f}% change exceeds {tolerance_pct}% tolerance)"
            ),
        }

    def _check_geometry_bounds(self, bounds: dict[str, float]) -> dict[str, Any]:
        """Check if geometry bounds are within Denmark."""
        from common.crs_utils import (
            DENMARK_BOUNDS_UTM,
            DENMARK_BOUNDS_WGS84,
            detect_crs_from_bounds,
        )

        crs, _ = detect_crs_from_bounds(
            bounds["min_x"], bounds["max_x"], bounds["min_y"], bounds["max_y"]
        )

        if crs == "EPSG:4326":
            expected = DENMARK_BOUNDS_WGS84
        elif crs == "EPSG:25832":
            expected = DENMARK_BOUNDS_UTM
        else:
            return {
                "name": "geometry_bounds",
                "passed": False,
                "message": f"Unknown CRS detected from bounds: {bounds}",
            }

        within_bounds = (
            bounds["min_x"] >= expected["min_x"] - 1
            and bounds["max_x"] <= expected["max_x"] + 1
            and bounds["min_y"] >= expected["min_y"] - 1
            and bounds["max_y"] <= expected["max_y"] + 1
        )

        return {
            "name": "geometry_bounds",
            "bounds": bounds,
            "expected_crs": crs,
            "passed": within_bounds,
            "message": (
                f"Geometry bounds within Denmark ({crs})"
                if within_bounds
                else f"Geometry bounds outside Denmark: {bounds}"
            ),
        }

    def _compute_checksum(self, df: Any) -> str:
        """Compute MD5 checksum of dataframe for quick comparison."""
        # Convert to bytes and hash
        data_bytes = df.to_csv(index=False).encode("utf-8")
        return hashlib.md5(data_bytes).hexdigest()

    def _extract_geometry_bounds(
        self, df: Any, geometry_column: str
    ) -> tuple[dict[str, float], str | None]:
        """Extract geometry bounds from a dataframe with WKB geometry."""
        # This is a simplified implementation - full version would use shapely
        # For now, return None and let DuckDB version handle it
        return None, None

    def _save_to_local(self, metrics: BaselineMetrics) -> str:
        """Save baseline to local filesystem."""
        path = Path(self.baseline_path) / metrics.dataset_name / "metrics.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(metrics.to_dict(), f, indent=2)
        logger.info(f"Saved baseline to {path}")
        return str(path)

    def _load_from_local(self, dataset_name: str) -> BaselineMetrics | None:
        """Load baseline from local filesystem."""
        path = Path(self.baseline_path) / dataset_name / "metrics.json"
        if not path.exists():
            logger.warning(f"No baseline found at {path}")
            return None
        with path.open() as f:
            data = json.load(f)
        return BaselineMetrics.from_dict(data)

    def _save_to_gcs(self, metrics: BaselineMetrics) -> str:
        """Save baseline to GCS."""
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.baseline_path}/{metrics.dataset_name}/metrics.json"
        with fs.open(path, "w") as f:
            json.dump(metrics.to_dict(), f, indent=2)
        logger.info(f"Saved baseline to {path}")
        return path

    def _load_from_gcs(self, dataset_name: str) -> BaselineMetrics | None:
        """Load baseline from GCS."""
        import gcsfs

        fs = gcsfs.GCSFileSystem()
        path = f"{self.baseline_path}/{dataset_name}/metrics.json"
        try:
            with fs.open(path) as f:
                data = json.load(f)
            return BaselineMetrics.from_dict(data)
        except FileNotFoundError:
            logger.warning(f"No baseline found at {path}")
            return None
