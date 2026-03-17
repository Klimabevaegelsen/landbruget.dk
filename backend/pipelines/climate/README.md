# Agricultural Climate Tool

## What is this pipeline? (For non-technical readers)

### The Problem: Measuring Agriculture's Climate Impact

Agriculture is a significant source of greenhouse gas emissions — from livestock methane, manure management, fertilizer application, crop residue decomposition, and farm energy use. Understanding each farm's carbon footprint is essential for climate policy and for farmers looking to reduce their environmental impact.

### What This Pipeline Does

This pipeline calculates **farm-level CO2e (carbon dioxide equivalent) emissions** for Danish agricultural operations. It combines existing data about livestock, fields, crops, and fertilizer to produce a comprehensive emissions report for each farm (identified by CVR number).

It calculates emissions from:
- **Cattle** (kvæg): enteric methane, feed, ammonia, manure storage, housing, diesel, electricity
- **Pigs** (svin): enteric methane, feed, housing and storage, ammonia
- **Poultry** (fjerkræ): enteric methane, feed, mortality, housing, storage, bedding, heating, electricity, purchased animals, egg/broiler accounting
- **Fields** (marker): crop residues, fertilizer and nitrification inhibitors, liming, carbon balance, nitrate leaching, organogens (peat soils), ammonia
- **Imported inputs**: diesel for machinery, electricity, imported fertilizer

### Why This Data Matters

The results help:
- **Climate researchers** understand agriculture's contribution to Danish greenhouse gas emissions
- **Policymakers** develop evidence-based climate regulation for the agricultural sector
- **Farmers** identify their largest emission sources and prioritize reduction efforts
- **Citizens** understand the climate impact of food production

---

## Technical Overview

This is a **gold-stage-only pipeline** — it does not fetch new data. Instead, it consumes existing silver-layer data and applies emission calculation algorithms inspired by the Agricultural Climate Tool used in the Danish industry.

### Data Sources (consumed, not fetched)

- **Livestock data** (CHR) — animal counts, herd composition (from `silver/chr/`)
- **Field data** (FVM) — agricultural fields, crop types, areas (from `silver/fvm_marker_YYYY/`)
- **Fertilizer data** — application rates and types (from `silver/fertiliser/`)
- **Climate data** (DMI) — weather and climate data (optional)

### Reference Values

Emission factors and calculation constants are stored in:
- `constants/emission_factors.json` — emission factors for manure, housing, field application, grazing, crop residues, and energy
- `reference_values/` — additional reference data extracted from the official Agricultural Climate Tool documentation using Gemini 2.5 Pro

## Quick Start

Calculate emissions for a single farm:
```bash
python main.py --cvr 31373077 --year 2024
```

Calculate for all farms with data (with limit):
```bash
python main.py --cvr all --year 2024 --limit 10
```

Dry run (no output written):
```bash
python main.py --cvr 12345678 --year 2024 --dry-run
```

## CLI Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--cvr` | Yes | CVR number (8 digits) or `all` to process all farms |
| `--year` | Yes | Year to calculate emissions for (YYYY) |
| `--output` | No | Destination: `gold` (R2), `supabase` (database), or `both` (default) |
| `--dry-run` | No | Calculate emissions but don't write output |
| `--log-level` | No | DEBUG, INFO, WARNING, ERROR |
| `--limit` | No | Limit number of CVRs (useful with `--cvr all`) |

## Project Structure

```
climate/
├── main.py                   # Entry point
├── cli.py                    # CLI argument parsing
├── climate_calculator.py     # Main calculation orchestrator
├── data_loader.py            # R2 data loading (silver layer)
├── data_transformer.py       # Data preparation
├── output_writer.py          # R2 gold layer + Supabase output
├── batch_processor.py        # Batch processing for multiple CVRs
├── farm_data.py              # Farm data models
├── crop_parameters.py        # Crop-specific parameters
├── standardfaktorer.py       # Standard emission factors
├── constants/
│   └── emission_factors.json # Emission factors (manure, housing, energy, etc.)
├── emission_sources/         # Emission source modules
│   ├── base_source.py        # Base class
│   ├── manure.py             # Manure emissions
│   └── digestion.py          # Biogas digestion
├── formulas/                 # Calculation formulas by category
│   ├── kvaeg/                # Cattle formulas (enterisk_metan, foder, ammoniak, etc.)
│   ├── svin/                 # Pig formulas (enterisk_metan, foder, stald_og_lager, etc.)
│   ├── fjerkrae/             # Poultry formulas (aarshoeneberegner, foder, stald, etc.)
│   ├── marker/               # Field formulas (afgroederester, kalkning, kulstofbalance, etc.)
│   └── import/               # Import formulas (diesel, el, importeret_goedning)
├── reference_values/         # Reference data from official tool documentation
├── utils/                    # Utility functions (conversions)
└── tests/                    # Test suite
    └── compliance/           # Compliance tests (cattle, field)
```

## Components

### Data Loader

The `data_loader.py` module loads agricultural data from existing silver/gold layers on R2. See [DATA_LOADER_README.md](./DATA_LOADER_README.md) for full documentation.

```python
from data_loader import ClimateDataLoader

loader = ClimateDataLoader()
livestock = loader.load_livestock(cvr="31373077", year=2024)
fields = loader.load_fields(cvr="31373077", year=2024)
fertilizer = loader.load_fertilizer(cvr="31373077", year=2024)
```

### Output Writer

The `output_writer.py` writes results to R2 gold layer and optionally syncs to Supabase:

- **R2**: `landbruget-data/gold/carbon_emissions/<timestamp>/`
  - `emissions.parquet` — main report data
  - `categories.parquet` — emission categories breakdown
  - `metadata.json` — run metadata
- **Supabase** (optional): `carbon_emissions` and `climate_emission_categories` tables

## Schedule

On-demand execution — no automated schedule. Run manually or via GitHub Actions.
