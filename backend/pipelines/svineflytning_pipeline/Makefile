.ONE_SHELL:
.DEFAULT_GOAL := help

SHELL = /bin/bash

SRC_DIR = .

##@ Commands
.PHONY: help
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <command> \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: install
install: ## Install packages using uv
	@echo "🚀 Installing packages"
	@uv sync --group dev

.PHONY: test
test: ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run pytest .

.PHONY: coverage
coverage: ## Test the code with pytest and generate coverage report
	@echo "🚀 Testing code and creating coverage report"
	@uv run coverage run --source ${SRC_DIR} -m pytest .
	@uv run coverage report
	@uv run coverage html
	@echo "open file://`pwd`/htmlcov/index.html"

.PHONY: format
format: ## Format the code using ruff
	@echo "🚀 Formatting code: Running ruff"
	@uv run ruff check ${SRC_DIR} --select I --fix ;\
	uv run ruff format ${SRC_DIR}

.PHONY: lint
lint: ## Lint the code using ruff
	@echo "🚀 Linting code: Running ruff"
	@uv run ruff check ${SRC_DIR} ;\
	if [ $$? -eq 0 ]; then \
        echo "Linting successful"; \
    else \
        echo "Linting failed"; \
    fi

.PHONY: mypy
mypy:  ## Check type hints using mypy
	@echo "🚀 Checking type hints: Running mypy"
	@uv run mypy ${SRC_DIR} ; \
	if [ $$? -eq 0 ]; then \
		echo "Mypy checks successful"; \
	else \
		echo "Mypy checks failed"; \
	fi

.PHONY: check
check: format lint mypy test ## Run all code quality checks
	@echo "🚀 All checks completed"

.PHONY: clean
clean: ## Remove Python file artifacts
	@echo "🚀 Cleaning Python file artifacts"
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	rm -fr .pytest_cache
	rm -f .coverage
	rm -fr htmlcov/ 