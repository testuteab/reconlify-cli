.PHONY: install lint test e2e format run clean help

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_.-]+:.*## .*$$' $(MAKEFILE_LIST) | \
	grep -vE '^\.PHONY:' | \
	sort | \
	awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

install: ## Install all dependencies
	poetry install

lint: ## Run ruff linter
	poetry run ruff check src tests

format: ## Auto-fix lint issues
	poetry run ruff check --fix src tests
	poetry run ruff format src tests

test: ## Run unit tests
	poetry run pytest -m "not e2e"

e2e: ## Run E2E tests
	poetry run pytest -m e2e -q

run: ## Run reconify (usage: make run ARGS="run config.yaml")
	poetry run reconify $(ARGS)

clean: ## Remove build artifacts and caches
	rm -rf dist .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	rm -rf .artifacts/*
