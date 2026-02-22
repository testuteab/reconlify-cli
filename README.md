# Reconify

Local-first, rule-based data reconciliation CLI tool.

## Setup

```bash
poetry install
```

## Usage

```bash
poetry run reconify run config.yaml
```

The config file is a YAML document. Minimal example:

```yaml
type: row_match
```

## Development

```bash
poetry run pytest
poetry run ruff check src tests
```
