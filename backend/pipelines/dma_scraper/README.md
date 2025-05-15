# DMA Scraper Pipeline

## Overview

The DMA Scraper Pipeline fetches and processes company data from the Danish Business Authority (DMA) API. It extracts relevant fields, transforms them into our internal schema, and loads the results into our data warehouse for downstream analytics.

## Architecture

```text
GitHub Actions Workflow (.github/workflows/dma_pipeline.yml)
            │
            ▼
DMA Scraper (main.py) ──► fetch_company_data.py ──► Process & transform ──► Output CSV/DB
```

## Prerequisites

- Python 3.9+
- Required packages: listed in `requirements.txt`

## Directory Structure

```
backend/pipelines/dma_scraper/
├── main.py                # Entry point for pipeline execution
├── fetch_company_data.py  # Handles HTTP requests and pagination
├── .env.example           # Example environment variables
└── README.md              # This document
```

## Configuration

Copy `.env.example` to `.env` at the root of the `dma_scraper` folder and fill in your credentials:

```bash
# Other optional parameters:
# MAX_PAGES=100
# OUTPUT_PATH=output/companies.csv
```

## Usage

Install dependencies:

```bash
pip install . e
```

Run locally:

```bash
python backend/pipelines/dma_scraper/main.py
```

The script will:
1. Read API credentials from `.env`
2. Fetch company data page by page
3. Transform and validate records
4. Write output to the configured location

## Scheduling

This pipeline is scheduled monthly via GitHub Actions (see `.github/workflows/dma_pipeline.yml`).

