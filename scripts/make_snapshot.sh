#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT}/.artifacts"
OUT_FILE="${OUT_DIR}/RECONIFY_CLI_PROJECT_SNAPSHOT.txt"

mkdir -p "$OUT_DIR"

INCLUDE_PATHS=(
  "src"
  "tests"
  "docs"
  "examples"
  "Makefile"
  "pyproject.toml"
  "README.md"
  ".gitignore"
)

EXCLUDE_DIRS_REGEX='/(.git|.venv|dist|build|.pytest_cache|.ruff_cache|__pycache__|.artifacts)(/|$)'

INCLUDE_FILES_REGEX='(\.py|\.md|\.toml|\.yaml|\.yml|\.json|\.txt|\.ini|\.cfg|Makefile|Dockerfile)$'

{
  echo "# Reconify Project Snapshot"
  echo "# Generated at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "# Root: ${ROOT}"
  echo
} >"$OUT_FILE"

append_file() {
  local path="$1"
  local abs="${ROOT}/${path}"

  if [[ -d "$abs" ]]; then
    while IFS= read -r -d '' f; do
      if [[ "$f" =~ $EXCLUDE_DIRS_REGEX ]]; then
        continue
      fi

      local rel="${f#${ROOT}/}"
      if [[ "$rel" =~ $INCLUDE_FILES_REGEX ]]; then
        echo "================================================================================" >>"$OUT_FILE"
        echo "FILE: ${rel}" >>"$OUT_FILE"
        echo "================================================================================" >>"$OUT_FILE"
        LC_ALL=C sed -e 's/\r$//' "$f" >>"$OUT_FILE" || true
        echo -e "\n" >>"$OUT_FILE"
      fi
    done < <(find "$abs" -type f -print0 | sort -z)
  elif [[ -f "$abs" ]]; then
    local rel="$path"
    echo "================================================================================" >>"$OUT_FILE"
    echo "FILE: ${rel}" >>"$OUT_FILE"
    echo "================================================================================" >>"$OUT_FILE"
    LC_ALL=C sed -e 's/\r$//' "$abs" >>"$OUT_FILE" || true
    echo -e "\n" >>"$OUT_FILE"
  else
    echo "[WARN] Missing path: ${path}" >>"$OUT_FILE"
  fi
}

for p in "${INCLUDE_PATHS[@]}"; do
  append_file "$p"
done

echo "Wrote snapshot: ${OUT_FILE}"
echo "Lines: $(wc -l < "$OUT_FILE" | tr -d ' ')"