Reconify-cli V1 Context

This project follows strict V1 boundaries defined in:
- docs/PRD_v1.md
- docs/YAML_SCHEMA_v1.md
- docs/REPORT_SCHEMA_v1.md

Do not introduce:
- expression engines
- nested transforms
- DB connections
- Parquet
- UI
- SaaS

Performance target:
1M rows within ~60 seconds.

All engines must respect schema definitions.