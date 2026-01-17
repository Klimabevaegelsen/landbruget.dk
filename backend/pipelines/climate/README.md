# Agricultural Climate Tool

This is an implementation of a climate-calculation inspired by the Agricultural Climate Tool actually used in the industry. This pipeline is mainly a gold-stage pipeline as no new data is fetched.

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

- `--cvr` (required): CVR number (8 digits) or 'all' to process all farms
- `--year` (required): Year to calculate emissions for (YYYY)
- `--output`: Output destination - 'gold' (GCS), 'supabase' (database), or 'both' (default)
- `--dry-run`: Calculate emissions but don't write output (for testing)
- `--log-level`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `--limit`: Limit number of CVRs to process (useful with --cvr all)

## Components

### Data Loader

The `data_loader.py` module provides GCS integration for loading agricultural data from existing silver/gold layers:

- **Livestock data** (CHR) - Animal counts, herd information
- **Field data** (FVM) - Agricultural fields, crop types, areas
- **Fertilizer data** - Application rates and types
- **Climate data** (DMI) - Weather and climate data (optional)

See [DATA_LOADER_README.md](./DATA_LOADER_README.md) for full documentation.

Quick example:
```python
from data_loader import ClimateDataLoader

loader = ClimateDataLoader()
livestock = loader.load_livestock(cvr="31373077", year=2024)
fields = loader.load_fields(cvr="31373077", year=2024)
fertilizer = loader.load_fertilizer(cvr="31373077", year=2024)
```

## Reference values

The reference values have been extracted using Gemini 2.5 Pro from the pdf describing the Agricultural Climate Tool. They can be found in `/reference_values`.
