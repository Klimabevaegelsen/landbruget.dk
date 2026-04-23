"""Audited homepage statistics source registry."""

from dataclasses import dataclass


@dataclass(frozen=True)
class HomepageStatisticsSource:
    legacy_table: str
    status: str
    resolver: str
    count_mode: str
    note: str
    dataset_prefix: str | None = None
    filename: str = "data.parquet"
    fixed_path: str | None = None
    object_glob: str | None = None
    derived_kind: str | None = None


HOMEPAGE_STATS_SOURCES: tuple[HomepageStatisticsSource, ...] = (
    HomepageStatisticsSource(
        legacy_table="field_yearly_data",
        status="exact",
        resolver="fixed",
        count_mode="parquet_footer_sum",
        fixed_path="gold/field_production/latest/data.parquet",
        note="Current field production gold export mirrors the legacy field/year grain.",
    ),
    HomepageStatisticsSource(
        legacy_table="pesticide_applications",
        status="exact",
        resolver="glob",
        count_mode="parquet_footer_sum",
        object_glob="gold/pesticide_disaggregation_*/*/pesticide_disaggregation_*.parquet",
        note="All disaggregated field-level pesticide application parquet files.",
    ),
    HomepageStatisticsSource(
        legacy_table="company_pesticide_applications",
        status="exact",
        resolver="glob",
        count_mode="parquet_footer_sum",
        object_glob="silver/pesticides/*/pesticiddata_*.parquet",
        note="Company-level pesticide input exports.",
    ),
    HomepageStatisticsSource(
        legacy_table="employee_monthly_counts",
        status="approximate",
        resolver="latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="silver/cvr_employment",
        note="Approximated from current CVR employment parquet.",
    ),
    HomepageStatisticsSource(
        legacy_table="field_boundaries",
        status="approximate",
        resolver="nested_latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="silver/agricultural_fields_*",
        note="Approximated from latest agricultural fields export.",
    ),
    HomepageStatisticsSource(
        legacy_table="animal_transports",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="animal_transport_records",
        note="Approximated from the live pig and cattle movement parquet exports.",
    ),
    HomepageStatisticsSource(
        legacy_table="field_wetland_areas",
        status="approximate",
        resolver="nested_latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="gold/field_analysis_field_wetland_intersections_*",
        note="Approximated from latest wetland intersection export.",
    ),
    HomepageStatisticsSource(
        legacy_table="worker_yearly_summary",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="worker_yearly_summary",
        note="Derived from employment, work permits, and inspection data using legacy materialized-view semantics.",
    ),
    HomepageStatisticsSource(
        legacy_table="vet_events",
        status="approximate",
        resolver="latest_match",
        count_mode="parquet_footer_sum",
        object_glob="silver/chr/*/property_vet_events.parquet",
        note="Approximated from latest veterinary events parquet.",
    ),
    HomepageStatisticsSource(
        legacy_table="animal_capacity_log",
        status="approximate",
        resolver="latest_match",
        count_mode="parquet_footer_sum",
        object_glob="silver/chr/*/herd_sizes*.parquet",
        note="Approximated from herd size snapshots.",
    ),
    HomepageStatisticsSource(
        legacy_table="herds",
        status="exact",
        resolver="latest_match",
        count_mode="parquet_footer_sum",
        object_glob="silver/chr/*/herds*.parquet",
        note="Current CHR herds parquet.",
    ),
    HomepageStatisticsSource(
        legacy_table="company_leadership",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="company_leadership",
        note="Derived from cvr_persons JSON using the existing leadership role classification.",
    ),
    HomepageStatisticsSource(
        legacy_table="company_owners",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="company_owners",
        note="Derived from cvr_persons JSON using the existing owner role classification.",
    ),
    HomepageStatisticsSource(
        legacy_table="production_sites",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="production_sites",
        note="Derived from live CHR properties plus property owner/user tables.",
    ),
    HomepageStatisticsSource(
        legacy_table="site_yearly_summary",
        status="approximate",
        resolver="derived",
        count_mode="duckdb_sql",
        derived_kind="site_yearly_summary",
        note="Derived from production sites, herd sizes, and antibiotic usage using legacy materialized-view semantics.",
    ),
    HomepageStatisticsSource(
        legacy_table="pesticide_products",
        status="exact",
        resolver="latest_match",
        count_mode="parquet_footer_sum",
        object_glob="silver/bmd/*/pesticide_products.parquet",
        note="Current pesticide product catalog parquet.",
    ),
    HomepageStatisticsSource(
        legacy_table="field_bnbo_areas",
        status="approximate",
        resolver="nested_latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="gold/field_analysis_field_bnbo_intersections_*",
        note="Approximated from latest BNBO intersection export.",
    ),
    HomepageStatisticsSource(
        legacy_table="visa_yearly_counts",
        status="exact",
        resolver="latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="gold/work_permits",
        filename="work_permits.parquet",
        note="Current work permits gold export.",
    ),
    HomepageStatisticsSource(
        legacy_table="antibiotic_usage",
        status="exact",
        resolver="latest_match",
        count_mode="parquet_footer_sum",
        object_glob="silver/chr/*/antibiotic_usage.parquet",
        note="Current antibiotic usage parquet.",
    ),
    HomepageStatisticsSource(
        legacy_table="yearly_financials",
        status="exact",
        resolver="latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="gold/cvr_enrichment_financial_statements",
        filename="financial_statements.parquet",
        note="Current financial statements gold export.",
    ),
    HomepageStatisticsSource(
        legacy_table="incidents",
        status="exact",
        resolver="latest",
        count_mode="parquet_footer_sum",
        dataset_prefix="silver/arbejdstilsynet_inspections",
        filename="workplace_inspections.parquet",
        note="Current workplace inspections silver export with company mapping.",
    ),
)
