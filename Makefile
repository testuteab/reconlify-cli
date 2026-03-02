.PHONY: install lint test e2e format run build clean help \
       perf perf-gen perf-tabular perf-text perf-smoke perf-clean

help: ## Show this help
	@grep -E '^[a-zA-Z0-9_.-]+:.*## .*$$' $(MAKEFILE_LIST) | \
	grep -vE '^\.PHONY:' | \
	sort | \
	awk 'BEGIN {FS = ":.*## "}; {printf "  %-14s %s\n", $$1, $$2}'

install: ## Install all dependencies
	poetry install

lint: ## Run ruff linter
	poetry run ruff check src tests scripts

format: ## Auto-fix lint issues
	poetry run ruff check --fix src tests scripts
	poetry run ruff format src tests scripts

test: ## Run unit tests
	poetry run pytest -m "not e2e and not perf"

e2e: ## Run E2E tests
	poetry run pytest -m e2e -q

run: ## Run reconify (usage: make run ARGS="run config.yaml")
	poetry run reconify $(ARGS)

clean: ## Remove build artifacts and caches
	rm -rf dist .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
	rm -rf .artifacts/*

snapshot: ## Build a single-file PROJECT_SNAPSHOT into .artifacts
	@bash scripts/make_snapshot.sh

build: ## Run poetry build and pip install
	poetry build
	pip install dist/*.whl
	pip install --user --force-reinstall dist/*.whl

# ---------------------------------------------------------------------------
# Performance benchmarks
# ---------------------------------------------------------------------------

perf-gen: ## Generate perf fixtures (.artifacts/perf/)
	poetry run python scripts/perf/gen_perf_fixtures.py

perf: perf-gen ## Run full perf benchmark suite
	poetry run python scripts/perf/run_bench.py

perf-tabular: perf-gen ## Run perf benchmarks (tabular only)
	poetry run python scripts/perf/run_bench.py --filter tabular

perf-text: perf-gen ## Run perf benchmarks (text only)
	poetry run python scripts/perf/run_bench.py --filter text

perf-smoke: ## Run lightweight perf smoke tests
	poetry run pytest -m perf -q

perf-clean: ## Remove perf fixtures
	rm -rf .artifacts/perf .artifacts/perf_smoke
