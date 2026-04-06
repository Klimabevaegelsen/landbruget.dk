#!/bin/bash
# Build all three paper PDFs using pdflatex
# Usage: bash build_papers.sh (from any directory)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "=== Building Paper 1: Pesticide Disaggregation ==="
cd "$REPO_ROOT/backend/pipelines/unified_pipeline/docs"
pdflatex -interaction=nonstopmode pesticide_disaggregation_paper.tex
pdflatex -interaction=nonstopmode pesticide_disaggregation_paper.tex
echo "-> Output: $(pwd)/pesticide_disaggregation_paper.pdf"

echo ""
echo "=== Building Paper 2: Groundwater-Pesticide Correlation ==="
cd "$REPO_ROOT/backend/scripts/reports"
pdflatex -interaction=nonstopmode groundwater-pesticide-correlation-2026.tex
pdflatex -interaction=nonstopmode groundwater-pesticide-correlation-2026.tex
echo "-> Output: $(pwd)/groundwater-pesticide-correlation-2026.pdf"

echo ""
echo "=== Building Paper 3: PFAS-Agriculture Correlation ==="
pdflatex -interaction=nonstopmode groundwater_pfas_agricultural_correlation_paper.tex
pdflatex -interaction=nonstopmode groundwater_pfas_agricultural_correlation_paper.tex
echo "-> Output: $(pwd)/groundwater_pfas_agricultural_correlation_paper.pdf"

echo ""
echo "=== All PDFs built successfully ==="
