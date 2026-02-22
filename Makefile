.PHONY: install lint test format run clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

install: ## Install all dependencies
	poetry install

lint: ## Run ruff linter
	poetry run ruff check src tests

format: ## Auto-fix lint issues
	poetry run ruff check --fix src tests
	poetry run ruff format src tests

test: ## Run tests
	poetry run pytest

run: ## Run reconify (usage: make run ARGS="run config.yaml")
	poetry run reconify $(ARGS)

clean: ## Remove build artifacts and caches
	rm -rf dist .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
