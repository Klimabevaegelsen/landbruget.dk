# Unified Pipeline

## What is this pipeline? (For non-technical readers)

### The Problem: Fragmented Government Data

Danish agricultural and environmental data is spread across dozens of government agencies, each with their own systems, formats, and access methods. This fragmentation makes it difficult for anyone — journalist, researcher, or citizen — to get a comprehensive picture of Danish agriculture.

### What This Pipeline Does

The Unified Pipeline is the backbone of Landbruget.dk's data collection. It automatically:

1. **Connects to 15+ government data sources**: From cadastral land data to weather stations, from field boundaries to pesticide registrations
2. **Downloads and preserves raw data**: Stores original data exactly as received (Bronze layer)
3. **Cleans and standardizes**: Normalizes formats, validates geometries, and ensures data quality (Silver layer)
4. **Creates analytical products**: Joins datasets, calculates derived metrics, and produces analysis-ready outputs (Gold layer)

### Why This Data Matters

The results help:
- **Journalists** cross-reference land ownership, crop data, and environmental compliance
- **Researchers** analyze agricultural patterns, environmental impact, and policy effectiveness
- **Policymakers** make evidence-based decisions about land use and agricultural regulation
- **Citizens** understand how Danish farmland is used and regulated

---

## Technical Overview

A unified data pipeline for fetching and processing various Danish agricultural and environmental datasets. Uses a class-based architecture with Click CLI.

## Bronze Layer Sources

Each source is a class inheriting from `BronzeBase`:

| Source | Description | Data Type | Agency |
|--------|-------------|-----------|--------|
| `cadastral` | Cadastral land parcels | WFS | Datafordeleren |
| `agricultural_fields` | Field boundaries and crop data | ArcGIS REST API | Landbrugsstyrelsen |
| `bnbo_status` | Well protection areas (Boringsnære beskyttelsesområder) | XML/SOAP API | Miljøstyrelsen |
| `spf_su` | Herd health and salmonella data | WFS | SPF-SU |
| `soil_types` | Soil type polygons | WFS | Miljøportalen |
| `dmi` | Precipitation and evaporation data | GovCloud API | DMI |
| `dagi` | Administrative boundaries | WFS | Datafordeleren |
| `dst` | Agricultural statistics | API | Danmarks Statistik |
| `fvm_wfs` | Agricultural authority data | WFS | FVM |
| `geus_dataverse_pesticides` | Borehole pesticide data | Dataverse API | GEUS |
| `grukos` | Agricultural classification | API | Landbrugsstyrelsen |
| `jordbrugsanalyser` | Agricultural analysis data | API | Landbrugsstyrelsen |
| `water_projects` | Water management projects | WFS | Miljøstyrelsen |
| `water_typology` | Water type classifications | WFS | Miljøstyrelsen |
| `wetlands` | Wetland areas | WFS | Miljøstyrelsen |
| `cvr_bronze` | Company registration data | API | CVR |

## Silver Layer

Each bronze source has a corresponding silver processor that:
- Validates and fixes geometries
- Standardizes column names to snake_case
- Cleans and validates data types
- Performs data completeness analysis
- Outputs GeoParquet format

## Gold Layer

Analysis-ready products that join multiple datasets:

| Gold Module | Description |
|-------------|-------------|
| `field_area_analysis` | Field area calculations with soil/water/property intersections |
| `field_production` | Crop production analysis |
| `pesticide_compliance` | Regulatory compliance analysis |
| `pesticide_disaggregation` | Pesticide distribution by field |
| `pesticide_proximity` | Proximity-based pesticide exposure |
| `pesticide_unit_sanitization` | Pesticide unit standardization |
| `cvr_enrichment` | Multi-step CVR company enrichment (geocoding, financial docs, P-numbers) |
| `nles5_nitrogen_estimation` | NLES5 nitrogen washout estimation |
| `arbejdstilsynet_inspections` | Labor authority inspections (gold processing) |
| `dst_field_crop_mapping` | DST to field crop type mapping |
| `property_cadastral_merge` | Property and cadastral data merge |
| `worker_safety` | Worker safety analysis |
| `work_permits` | Work permit data |
| `cvr_geometry_datasets` | CVR geometry dataset generation |

## Usage

Run a specific source and stage:

```bash
python -m unified_pipeline -s <source> -j <job>
```

- `<source>`: any source name from the tables above
- `<job>`: `bronze`, `silver`, or `all`

Examples:
```bash
# Bronze stage for cadastral data
python -m unified_pipeline -s cadastral -j bronze

# Silver stage for soil types
python -m unified_pipeline -s soil_types -j silver

# Both stages for agricultural fields
python -m unified_pipeline -s agricultural_fields -j all
```

## Prerequisites

1. Python 3.11+
2. R2 credentials (or GCS service account key for legacy setups)
3. Create a `.env` file based on `.env.example`:
   ```
   R2_BUCKET=<your-r2-bucket>
   MAX_CONCURRENT=20
   SAVE_LOCAL=False
   ```
4. Install dependencies:
   ```bash
   uv pip install -e .
   ```

## Configuration

Environment variables:
- `R2_BUCKET`: Cloudflare R2 bucket for data storage (falls back to `GCS_BUCKET`)
- `MAX_CONCURRENT`: Number of concurrent HTTP requests for bronze fetching (default: 20)
- `SAVE_LOCAL`: Set to `true` to save data locally under `/tmp` instead of R2

## Architecture

```
src/unified_pipeline/
├── app.py                    # Click CLI entry point
├── base/                     # Base classes
│   ├── bronze_base.py        # BronzeBase class
│   ├── silver_base.py        # SilverBase class
│   └── gold_base.py          # GoldBase class
├── bronze/                   # Bronze source modules (15+)
├── silver/                   # Silver processing modules
├── gold/                     # Gold analytical modules (14+)
├── common/                   # Shared utilities (geometry_validator, schema_manager)
├── model/                    # Pydantic config models
├── util/                     # Utilities (CVR API, geocoding, DAWA, logging)
└── core/                     # Core features (incremental processing)
```

## Schedule

- **Weekly**: Monday at 2 AM UTC (GitHub Actions)
- **Manual**: Can be triggered via `workflow_dispatch`
- Various gold layer modules run on separate schedules
