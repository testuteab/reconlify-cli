# Column Mapping — V1.1

## Feature Summary

Column mapping allows comparing datasets where source and target use different
column names for the same data. This is common when reconciling exports from
different systems (e.g., ERP vs ledger, source DB vs data warehouse).

## Semantics

- **Logical column name** = source-side column name (canonical identifier)
- **Target physical column** = resolved via `column_mapping` or defaults to
  the logical name
- All config fields (`keys`, `compare`, `tolerance`, `string_rules`,
  `ignore_columns`, `normalization`) use logical/source-side column names
- `column_mapping` only affects target-side column resolution

### Config format

```yaml
column_mapping:
  source_column: target_column
  trade_id: id
  amount: total_amount
```

### Resolution logic

For each logical column:
- **Source side:** use the logical column name directly (or normalization output)
- **Target side:** `column_mapping.get(logical, logical)`

## Examples

### Basic key + value mapping

```yaml
type: tabular
source: trades_erp.csv
target: trades_ledger.csv

keys:
  - trade_id

column_mapping:
  trade_id: id
  amount: total_amount

tolerance:
  amount: 0.01
```

- `trade_id` in source joins against `id` in target
- `amount` in source compares against `total_amount` in target with tolerance

### Normalization + mapping

```yaml
type: tabular
source: source.csv
target: target.csv

keys:
  - id

column_mapping:
  full_name: customer_full_name

normalization:
  full_name:
    - op: concat
      args: [first_name, " ", last_name]
    - op: trim

compare:
  include_columns:
    - full_name

string_rules:
  full_name:
    - trim
    - case_insensitive
```

- `full_name` is generated on the source side from `first_name` + `last_name`
- It maps to `customer_full_name` on the target side
- String rules (trim + case) apply to both sides using the logical name

## Implementation

Column mapping is resolved during the **projection phase** of the tabular
engine. Target columns are aliased to their logical names at this stage, so
all downstream logic (filtering, normalization, comparison, sampling) operates
in the logical column namespace without additional mapping lookups.

### Key changes

- **models.py:** `TabularConfig.column_mapping: dict[str, str]` with
  validation (no empty keys/values, no duplicate target columns)
- **tabular_engine.py:** target projection aliases mapped columns to logical
  names; validation for missing target columns and aliasing collisions
- **report:** `details.column_mapping` shows effective mappings in the report
- **cli.py / report.py:** pass through and serialize column_mapping

## Report

When column mapping is configured, the report includes:

```json
{
  "details": {
    "keys": ["trade_id"],
    "compared_columns": ["amount", "customer_name"],
    "column_mapping": {
      "trade_id": "id",
      "amount": "total_amount",
      "customer_name": "client_name"
    }
  }
}
```

- `keys` and `compared_columns` use logical/source-side names
- `column_mapping` is omitted when empty (no mappings configured)

## Known Limitations

- **Source-side normalization only.** Target columns are not transformed.
- **No many-to-one mapping.** Each logical column maps to exactly one target
  column.
- **No one-to-many mapping.** Each target column can only be mapped from one
  logical column.
- **No target-side transforms.** Column mapping is a pure rename; no
  expressions or computed columns on the target side.
- **No cross-column mapping validation at config time.** Validation that
  mapped target columns exist happens at runtime when the CSV is loaded.
