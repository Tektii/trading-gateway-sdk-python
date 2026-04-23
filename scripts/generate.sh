#!/usr/bin/env bash
# Generate Pydantic v2 models from the Trading Gateway OpenAPI spec.
#
# Usage:
#   ./scripts/generate.sh          # Generate (overwrites _generated/models.py)
#   ./scripts/generate.sh --check  # Check for drift (exits 1 if models differ)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
# The OpenAPI spec is vendored at the repo root so CI has a self-contained
# source of truth. To pick up upstream gateway changes, copy the newer
# ../tektii-gateway/openapi.json over this file, rerun this script, and
# commit both the spec and the regenerated models together.
OPENAPI_SPEC="${PROJECT_DIR}/openapi.json"
OUTPUT_FILE="${PROJECT_DIR}/src/tektii_gateway/_generated/models.py"

if [ ! -f "$OPENAPI_SPEC" ]; then
    echo "Error: OpenAPI spec not found at $OPENAPI_SPEC"
    echo "A vendored openapi.json should live at the repo root."
    exit 1
fi

generate() {
    uv run datamodel-codegen \
        --input "$OPENAPI_SPEC" \
        --output "$1" \
        --output-model-type pydantic_v2.BaseModel \
        --target-python-version 3.11 \
        --use-standard-collections \
        --use-union-operator \
        --field-constraints \
        --capitalise-enum-members \
        --use-decimal
}

normalise() {
    # Strip the generator header (4 lines: 3 comments + trailing blank) so
    # timestamp churn doesn't cause false-positive drift in CI.
    tail -n +5 "$1"
}

if [ "${1:-}" = "--check" ]; then
    TEMP_FILE=$(mktemp)
    trap 'rm -f "$TEMP_FILE"' EXIT
    generate "$TEMP_FILE"
    if diff -q <(normalise "$OUTPUT_FILE") <(normalise "$TEMP_FILE") > /dev/null 2>&1; then
        echo "Models are up to date."
        exit 0
    else
        echo "Models are out of date! Run ./scripts/generate.sh to regenerate."
        diff <(normalise "$OUTPUT_FILE") <(normalise "$TEMP_FILE") || true
        exit 1
    fi
else
    generate "$OUTPUT_FILE"
    echo "Generated models at $OUTPUT_FILE"
fi
