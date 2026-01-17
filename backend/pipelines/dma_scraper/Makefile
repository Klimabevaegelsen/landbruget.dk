# Makefile for DMA Scraper Pipeline
# Standardized code quality commands using Astral toolchain (ruff, uv, ty)

.DEFAULT_GOAL := help
SHELL = /bin/bash

SRC_DIR = .
TEST_DIR = tests

##@ Help
.PHONY: help
help:  ## Display this help
	@echo "Available targets:"
	@echo "  install     - Install dependencies with uv"
	@echo "  format      - Format code with ruff"
	@echo "  lint        - Lint code with ruff"
	@echo "  typecheck   - Type check with ty"
	@echo "  test        - Run tests with pytest"
	@echo "  check       - Run all quality checks"
	@echo "  clean       - Remove build artifacts"

##@ Setup
.PHONY: install
install:  ## Install dependencies using uv
	@echo "🚀 Installing dependencies with uv"
	@uv sync

##@ Code Quality
.PHONY: format
format:  ## Format code using ruff
	@echo "🚀 Formatting code with ruff"
	@uv run ruff check $(SRC_DIR) --select I --fix
	@uv run ruff format $(SRC_DIR)

.PHONY: lint
lint:  ## Lint code using ruff
	@echo "🚀 Linting code with ruff"
	@uv run ruff check $(SRC_DIR)

.PHONY: typecheck
typecheck:  ## Type check using ty (Astral's type checker)
	@echo "🚀 Type checking with ty"
	@uv run ty check $(SRC_DIR)

##@ Testing
.PHONY: test
test:  ## Run tests with pytest
	@echo "🚀 Running tests with pytest"
	@uv run pytest $(TEST_DIR) -v

##@ Quality Suite
.PHONY: check
check: lint typecheck test  ## Run all quality checks (lint, typecheck, test)
	@echo "✅ All checks passed"

##@ Cleanup
.PHONY: clean
clean:  ## Remove build artifacts
	@echo "🚀 Cleaning build artifacts"
	@rm -rf build/ dist/ *.egg-info .pytest_cache .ruff_cache __pycache__
	@find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name '*.pyc' -delete 2>/dev/null || true
