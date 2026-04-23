#!/usr/bin/env bash
# Generate Pydantic v2 models from the Trading Gateway OpenAPI spec.
#
# Usage:
#   ./scripts/generate.sh          # Generate (overwrites _generated/models.py)
#   ./scripts/generate.sh --check  # Check for drift (exits 1 if models differ)
#
# Source of the spec (in order of precedence):
#   1. $OPENAPI_SPEC — local path or URL, if set
#   2. Remote fetch from the public trading-gateway repo:
#         https://raw.githubusercontent.com/Tektii/trading-gateway/${GATEWAY_REF:-main}/openapi.json
#
# Environment variables:
#   OPENAPI_SPEC  Override the spec source with a local path or arbitrary URL.
#   GATEWAY_REF   Git ref (branch, tag, or SHA) to pull the spec from. Defaults to "main".

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_FILE="${PROJECT_DIR}/src/tektii/_generated/models.py"

GATEWAY_REF="${GATEWAY_REF:-main}"
DEFAULT_REMOTE_URL="https://raw.githubusercontent.com/Tektii/trading-gateway/${GATEWAY_REF}/openapi.json"
SPEC_SOURCE="${OPENAPI_SPEC:-$DEFAULT_REMOTE_URL}"

FETCHED_SPEC=""
cleanup() {
    if [ -n "$FETCHED_SPEC" ] && [ -f "$FETCHED_SPEC" ]; then
        rm -f "$FETCHED_SPEC"
    fi
}
trap cleanup EXIT

resolve_spec() {
    # Resolve $SPEC_SOURCE to a local file path, downloading if necessary.
    # Sets $RESOLVED_SPEC.
    case "$SPEC_SOURCE" in
        http://*|https://*)
            FETCHED_SPEC=$(mktemp)
            echo "Fetching OpenAPI spec from $SPEC_SOURCE" >&2
            if ! curl -fsSL "$SPEC_SOURCE" -o "$FETCHED_SPEC"; then
                echo "Error: failed to fetch OpenAPI spec from $SPEC_SOURCE" >&2
                exit 1
            fi
            RESOLVED_SPEC="$FETCHED_SPEC"
            ;;
        *)
            if [ ! -f "$SPEC_SOURCE" ]; then
                echo "Error: OpenAPI spec not found at $SPEC_SOURCE" >&2
                exit 1
            fi
            RESOLVED_SPEC="$SPEC_SOURCE"
            ;;
    esac
}

generate() {
    uv run datamodel-codegen \
        --input "$RESOLVED_SPEC" \
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

resolve_spec

if [ "${1:-}" = "--check" ]; then
    TEMP_FILE=$(mktemp)
    trap 'rm -f "$TEMP_FILE"; cleanup' EXIT
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
