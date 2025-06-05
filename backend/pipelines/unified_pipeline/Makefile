.ONE_SHELL:
.DEFAULT_GOAL := help

SHELL = /bin/bash

# docker tag
GCLOUD_DOCKER_URL = asia-south1-docker.pkg.dev
DOCKER_IMAGE_NAME = $(GCLOUD_DOCKER_URL)/landbruget.dk/unified_pipeline
DOCKER_TAG = latest

SRC_DIR = src
MAIN_DIR = $(SRC_DIR)/unified_pipeline
TEST_DIR = $(SRC_DIR)/tests

DOCKERFILE = Dockerfile

##@ Commands
.PHONY: help
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <command> \033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: install
install: ## Create the virtual environment and install packages
	@echo "🚀 Creating virtual environment using uv and installing packages"
	@uv sync

.PHONY: build
build: clean-build  ## Build wheel file
	@echo "🚀 Creating wheel file"
	@uvx --from build pyproject-build --installer uv

.PHONY: test
test: clean-test  ## Test the code with pytest
	@echo "🚀 Testing code: Running pytest"
	@uv run pytest ${TEST_DIR}

.PHONY: coverage
coverage: clean-test  ## Test the code with pytest and generate coverage report
	@echo "🚀 Testing code and creating coverage report"
	@uv run coverage run --source ${MAIN_DIR} -m pytest ${TEST_DIR}
	@uv run coverage report
	@uv run coverage html
	@echo "open file://`pwd`/htmlcov/index.html"

.PHONY: clean
clean: clean-build clean-pyc clean-test clean-docs  ## Remove all build, test, coverage and Python artifacts

.PHONY: clean-build
clean-build:  ## Remove build artifacts
	@echo "🚀 Cleaning build artifacts"
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -rf {} +

.PHONY: clean-pyc
clean-pyc:  ## Remove Python file artifacts
	@echo "🚀 Cleaning Python file artifacts"
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

.PHONY: clean-test
clean-test:  ## Remove test and coverage artifacts
	@echo "🚀 Cleaning test and coverage artifacts"
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

.PHONY: clean-docs
clean-docs:  ## Remove documentation artifacts
	@echo "🚀 Cleaning documentation artifacts"
	rm -fr site/

.PHONY: format
format: ## Format the code using ruff
	@echo "🚀 Formatting code: Running ruff"
	@uv run ruff check ${SRC_DIR} --select I --fix ;\
	uv run ruff format ${SRC_DIR}

.PHONY: lint
lint: ## Lint the code using flake8
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
	@uv run mypy --cache-fine-grained $(SRC_DIR) ; \
	if [ $$? -eq 0 ]; then \
		echo "Mypy checks successful"; \
	else \
		echo "Mypy checks failed"; \
	fi

.PHONY: check
check: ## Run code quality tools.
	@echo "🚀 Checking lock file consistency with 'pyproject.toml'"
	@uv sync --locked
	@echo "🚀 Static type checking: Running mypy"
	@uv run mypy
	@echo "🚀 Checking for obsolete dependencies: Running deptry"
	@uv run deptry .

.PHONY: docs-test
docs-test: ## Test if documentation can be built without warnings or errors
	@echo "🚀 Testing documentation: Running mkdocs"
	@uv run mkdocs build -s

.PHONY: docs
docs: ## Build and serve the documentation
	@echo "🚀 Building documentation: Running mkdocs"
	@uv run mkdocs serve

.PHONY: docker-build
docker-build: build  ## Build docker image
	@echo "🚀 Building docker image"
	docker build -t $(DOCKER_IMAGE_NAME):${DOCKER_TAG} .

.PHONY: docker-push
docker-push:  ## Push docker image
	@echo "🚀 Pushing docker image"
	gcloud auth configure-docker $(GCLOUD_DOCKER_URL)
	docker push $(DOCKER_IMAGE_NAME):${DOCKER_TAG} ; \
	if [ $$? -eq 0 ]; then \
		echo "Docker image pushed successfully"; \
	else \
		echo "Docker image push failed"; \
		echo "Please check if you are logged in to gcloud"; \
		echo "Use: gcloud auth login"; \
	fi

.PHONY: docker-compose-up
docker-compose-up:  ## Run docker compose up
	@echo "🚀 Running docker compose -d up ";
	@docker compose -f $(DOCKER_COMPOSE_FILE) up -d

.PHONY: docker-compose-down
docker-compose-down:  ## Run docker compose down
	@echo "🚀 Running docker-compose down";
	@docker compose -f $(DOCKER_COMPOSE_FILE) down
