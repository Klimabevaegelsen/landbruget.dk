# DMA Scraper Pipeline

## What is this pipeline? (For non-technical readers)

### The Problem: Tracking Environmentally Regulated Farms

Danish farms and other businesses that may impact the environment are registered in the **DMA** (Digital MiljøAdministration) system operated by **Miljøstyrelsen** (the Danish Environmental Protection Agency) at [dma.mst.dk](https://dma.mst.dk). This registry tracks which companies are under environmental oversight, their inspection history, and any enforcement actions taken.

### What This Pipeline Does

This pipeline automatically:

1. **Scrapes the DMA registry**: Fetches company listings page by page from the DMA search interface
2. **Collects detailed records**: For each company, retrieves inspection reports (tilsyn), enforcement actions (håndhævelser), and regulatory decisions (afgørelser)
3. **Structures the data**: Transforms raw scraped data into clean, queryable formats

### Why This Data Matters

The results help:
- **Environmental journalists** investigate which farms have been inspected and what violations were found
- **Regulatory researchers** analyze patterns in environmental enforcement
- **Policymakers** assess the effectiveness of environmental oversight
- **Citizens** learn about environmental compliance of farms in their area

---

## Technical Overview

The pipeline scrapes the Danish Environmental Protection Agency's DMA registry at `https://dma.mst.dk/soeg/page` using HTTP POST requests with pagination.

## Architecture

```text
GitHub Actions Workflow (.github/workflows/dma_pipeline.yml)
            │
            ▼
GCP VM provisioned by GitHub Actions
            │
            ▼
uv workspace package (dma-scraper) installed on the VM
            │
            ▼
DMA Scraper (main.py, ENVIRONMENT=production)
    ├── bronze/fetch_company_data.py    → Paginated company list scraping
    ├── bronze/fetch_company_detail.py  → Per-company detail scraping (inspections, PDFs)
    └── silver/transformation.py        → Transform to Parquet + CVR extraction
```

The GitHub workflow provisions a GCP VM for the long-running scrape, copies the checked-out workspace to the VM, validates R2 credentials, and launches the VM runner as a detached background process because full DMA scrapes can outlast GitHub Actions runtime limits. The VM installs the `dma-scraper` workspace package with `uv`, runs the DMA tests, executes `main.py`, writes production output to R2 under `bronze/dma/` and `silver/dma/`, then deletes itself when `auto_shutdown` is enabled. The workflow refuses to launch if another DMA VM is already active, deletes stale terminated DMA VMs before starting, and sets a hard 72-hour GCE auto-delete limit as a backstop.

## Directory Structure

```
dma_scraper/
├── main.py                          # Entry point
├── enhanced_dma_detail_scraper.py   # Enhanced async detail scraper
├── bronze/
│   ├── fetch_company_data.py        # Company list pagination (POST to dma.mst.dk/soeg/page)
│   └── fetch_company_detail.py      # Per-company detail + PDF scraping
├── silver/
│   └── transformation.py            # Data transformation to Parquet
├── prototypes/
│   └── dma_environmental_permits_analysis_prototype.py
├── .env.example
├── Dockerfile
├── Makefile
└── pyproject.toml
```

## Data Collected

- Company name, CVR number, CHR code
- Address and municipality
- Regulatory classification
- Inspection records (tilsyn)
- Enforcement actions (håndhævelser)
- Regulatory decisions and PDFs (afgørelser)

## Prerequisites

- Python 3.9+
- Required packages: listed in `pyproject.toml`

## Configuration

Copy `.env.example` to `.env` and fill in credentials:

```bash
cp .env.example .env
```

## Usage

```bash
uv pip install -e .
python main.py
```

The script will:
1. Fetch company data page by page from the DMA registry
2. Collect detailed inspection and enforcement data per company
3. Transform and validate records
4. Write output to the configured location

## Scheduling

This pipeline is scheduled monthly via GitHub Actions (see `.github/workflows/dma_pipeline.yml`).
