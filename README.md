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

The config file is a YAML document. Minimal tabular example:

```yaml
type: tabular
source: source.csv
target: target.csv
keys:
  - id
```

Minimal text example:

```yaml
type: text
source: source.txt
target: target.txt
```

TSV files are supported via the configurable delimiter:

```yaml
type: tabular
source: source.tsv
target: target.tsv
keys:
  - id
csv:
  delimiter: "\t"
  header: true
  encoding: "utf-8"
```

## Development

```bash
poetry run pytest
poetry run ruff check src tests
```
