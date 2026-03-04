# E2E Tests

End-to-end tests that invoke the `reconlify` CLI as a subprocess and validate
the JSON report output.

## Running

```bash
make e2e                      # via Makefile
poetry run pytest -m e2e -q   # directly
```

## Structure

```
tests/e2e/
├── conftest.py          # e2e_runner fixture + helpers
├── test_text_e2e.py     # test functions
└── cases/
    └── <case_id>/
        ├── config.yaml  # reconlify YAML config (source/target use relative paths)
        ├── source.txt   # source fixture (may be absent for runtime-generated cases)
        └── target.txt   # target fixture
```

All generated outputs (resolved configs, reports) go to `.artifacts/e2e/<case_id>/`.
