#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BRIDGE_URL=${BRIDGE_URL:-http://127.0.0.1:8787}
AUTO_AUTH=1
KEEP_ARTIFACTS=0
PROBE=1
ALL=0
ALLOW_ATTENTION_REQUIRED=0
OUTPUT_DIR=
PROVIDER_ID=${PROVIDER_ID:-}

usage() {
    cat <<'EOF'
Usage: scripts/acp-smoke.sh [options]

Runs a local ACP readiness/smoke check against a running bridge server:
1. checks /health
2. auto-provisions client auth when needed
3. fetches /agents and verifies the ACP agent summary is present
4. fetches /agents/acp/diagnostic
5. optionally runs a live ACP probe (?probe=true)

Tip: start from config/settings.acp.example.json when preparing ~/.omni-code/settings.json.

Options:
  --bridge-url URL         Bridge base URL. Default: http://127.0.0.1:8787
  --provider-id ID         Diagnose a specific ACP server id.
  --all                    Return diagnostics for all configured ACP servers.
  --no-probe               Skip ?probe=true and only inspect static diagnostics.
  --allow-attention-required
                           Treat readiness=attention_required as non-fatal.
  --client-id ID           Approved client id. If omitted, the script auto-provisions one.
  --token TOKEN            Approved bearer token. If omitted, the script auto-provisions one.
  --output-dir DIR         Keep JSON outputs in DIR.
  --keep-artifacts         Keep generated outputs.
  --no-auto-auth           Require --client-id and --token instead of provisioning auth.
  -h, --help               Show this help.

Environment overrides:
  BRIDGE_URL
  BRIDGE_CLIENT_ID
  BRIDGE_TOKEN
  PROVIDER_ID
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --bridge-url)
            BRIDGE_URL=$2
            shift 2
            ;;
        --provider-id)
            PROVIDER_ID=$2
            shift 2
            ;;
        --all)
            ALL=1
            shift
            ;;
        --no-probe)
            PROBE=0
            shift
            ;;
        --allow-attention-required)
            ALLOW_ATTENTION_REQUIRED=1
            shift
            ;;
        --client-id)
            BRIDGE_CLIENT_ID=$2
            shift 2
            ;;
        --token)
            BRIDGE_TOKEN=$2
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR=$2
            KEEP_ARTIFACTS=1
            shift 2
            ;;
        --keep-artifacts)
            KEEP_ARTIFACTS=1
            shift
            ;;
        --no-auto-auth)
            AUTO_AUTH=0
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

need_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "Missing required command: $1" >&2
        exit 1
    fi
}

need_cmd curl
need_cmd jq

if [ -z "${OUTPUT_DIR:-}" ]; then
    OUTPUT_DIR=${TMPDIR:-/tmp}/omni-code-bridge-acp-smoke-$$
fi
mkdir -p "$OUTPUT_DIR"

cleanup() {
    if [ "$KEEP_ARTIFACTS" -eq 0 ]; then
        rm -rf "$OUTPUT_DIR"
    else
        echo "Artifacts kept in: $OUTPUT_DIR"
    fi
}
trap cleanup EXIT INT TERM

auth_curl() {
    curl -fsS \
        -H "Authorization: Bearer $BRIDGE_TOKEN" \
        -H "x-omni-code-client-id: $BRIDGE_CLIENT_ID" \
        "$@"
}

approve_client_auth() {
    request_id=$1
    if command -v omni-code-bridge >/dev/null 2>&1; then
        omni-code-bridge client-auth approve --request-id "$request_id"
        return
    fi
    if [ -x "$ROOT_DIR/target/debug/omni-code-bridge" ]; then
        "$ROOT_DIR/target/debug/omni-code-bridge" client-auth approve --request-id "$request_id"
        return
    fi
    (
        cd "$ROOT_DIR"
        cargo run --quiet -- client-auth approve --request-id "$request_id"
    )
}

check_bridge_health() {
    echo "Checking bridge health at $BRIDGE_URL"
    curl -fsS "$BRIDGE_URL/health" | tee "$OUTPUT_DIR/health.json" >/dev/null
}

bootstrap_auth() {
    if [ -n "${BRIDGE_CLIENT_ID:-}" ] && [ -n "${BRIDGE_TOKEN:-}" ]; then
        return
    fi

    if [ "$AUTO_AUTH" -ne 1 ]; then
        echo "BRIDGE_CLIENT_ID and BRIDGE_TOKEN are required when --no-auto-auth is set" >&2
        exit 1
    fi

    need_cmd cargo

    BRIDGE_CLIENT_ID=acp-smoke-$(date +%s)
    request_body=$(jq -nc \
        --arg client_id "$BRIDGE_CLIENT_ID" \
        --arg device_name "acp-smoke" \
        '{client_id:$client_id,device_name:$device_name}')

    echo "Requesting client auth for $BRIDGE_CLIENT_ID"
    curl -fsS \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$request_body" \
        "$BRIDGE_URL/client-auth/requests" >"$OUTPUT_DIR/client-auth-request.json"

    request_id=$(jq -r '.data.request_id // empty' "$OUTPUT_DIR/client-auth-request.json")
    if [ -z "$request_id" ]; then
        echo "Failed to create client auth request" >&2
        exit 1
    fi

    echo "Approving client auth request $request_id"
    approve_client_auth "$request_id" >"$OUTPUT_DIR/client-auth-approve.txt"

    attempts=0
    while [ "$attempts" -lt 20 ]; do
        curl -fsS "$BRIDGE_URL/client-auth/requests/$request_id" >"$OUTPUT_DIR/client-auth-status.json"
        BRIDGE_TOKEN=$(jq -r '.data.token // empty' "$OUTPUT_DIR/client-auth-status.json")
        if [ -n "$BRIDGE_TOKEN" ]; then
            export BRIDGE_CLIENT_ID BRIDGE_TOKEN
            return
        fi
        attempts=$((attempts + 1))
        sleep 1
    done

    echo "Timed out waiting for approved client token" >&2
    exit 1
}

fetch_agents() {
    echo "Fetching /agents"
    auth_curl "$BRIDGE_URL/agents" >"$OUTPUT_DIR/agents.json"
    agent_count=$(jq '.data | length' "$OUTPUT_DIR/agents.json")
    echo "Discovered $agent_count agent entries"
    jq -e '.data[] | select(.id == "acp")' "$OUTPUT_DIR/agents.json" >"$OUTPUT_DIR/acp-agent-summary.json" || {
        echo "ACP agent summary not found in /agents response" >&2
        exit 1
    }
    readiness=$(jq -r '.readiness // "unknown"' "$OUTPUT_DIR/acp-agent-summary.json")
    installed=$(jq -r '.installed // false' "$OUTPUT_DIR/acp-agent-summary.json")
    echo "ACP agent summary readiness=$readiness installed=$installed"
    jq '{id, label, readiness, installed, installed_path, readiness_message, acp_diagnostic}' \
        "$OUTPUT_DIR/acp-agent-summary.json"

    case "$readiness" in
        ready)
            ;;
        attention_required)
            if [ "$ALLOW_ATTENTION_REQUIRED" -ne 1 ]; then
                echo "ACP agent summary is attention_required; rerun with --allow-attention-required if this is expected" >&2
                exit 1
            fi
            ;;
        *)
            echo "ACP agent summary readiness is not acceptable: $readiness" >&2
            exit 1
            ;;
    esac
}

build_diagnostic_url() {
    url="$BRIDGE_URL/agents/acp/diagnostic"
    sep='?'
    if [ "$ALL" -eq 1 ]; then
        url="${url}${sep}all=true"
        sep='&'
    fi
    if [ -n "${PROVIDER_ID:-}" ]; then
        encoded_provider=$(printf '%s' "$PROVIDER_ID" | jq -sRr @uri)
        url="${url}${sep}provider_id=${encoded_provider}"
        sep='&'
    fi
    if [ "$PROBE" -eq 1 ]; then
        url="${url}${sep}probe=true"
        sep='&'
    fi
    url="${url}${sep}refresh=true"
    printf '%s\n' "$url"
}

fetch_acp_diagnostic() {
    diagnostic_url=$(build_diagnostic_url)
    echo "Fetching ACP diagnostic: $diagnostic_url"
    auth_curl "$diagnostic_url" >"$OUTPUT_DIR/acp-diagnostic.json"

    if [ "$ALL" -eq 1 ]; then
        count=$(jq '.data | length' "$OUTPUT_DIR/acp-diagnostic.json")
        echo "Received $count ACP diagnostic items"
        jq '.data[] | {provider_id, is_default_selected, readiness, readiness_message, handshake_probe, diagnostic}' \
            "$OUTPUT_DIR/acp-diagnostic.json"
        validate_acp_diagnostic_all
        return
    fi

    jq '.data | {provider_id, is_default_selected, readiness, readiness_message, handshake_probe, diagnostic}' \
        "$OUTPUT_DIR/acp-diagnostic.json"

    readiness=$(jq -r '.data.readiness // "unknown"' "$OUTPUT_DIR/acp-diagnostic.json")
    probe_success=$(jq -r '.data.handshake_probe.success // "n/a"' "$OUTPUT_DIR/acp-diagnostic.json")
    echo "ACP diagnostic readiness=$readiness probe_success=$probe_success"
    validate_acp_diagnostic_one
}

validate_readiness_value() {
    readiness=$1
    context=$2
    case "$readiness" in
        ready)
            return 0
            ;;
        attention_required)
            if [ "$ALLOW_ATTENTION_REQUIRED" -eq 1 ]; then
                return 0
            fi
            echo "$context readiness is attention_required; rerun with --allow-attention-required if this is expected" >&2
            return 1
            ;;
        *)
            echo "$context readiness is not acceptable: $readiness" >&2
            return 1
            ;;
    esac
}

validate_acp_diagnostic_one() {
    readiness=$(jq -r '.data.readiness // "unknown"' "$OUTPUT_DIR/acp-diagnostic.json")
    validate_readiness_value "$readiness" "ACP diagnostic"

    if [ "$PROBE" -eq 1 ]; then
        attempted=$(jq -r '.data.handshake_probe.attempted // false' "$OUTPUT_DIR/acp-diagnostic.json")
        success=$(jq -r '.data.handshake_probe.success // false' "$OUTPUT_DIR/acp-diagnostic.json")
        if [ "$attempted" != "true" ] || [ "$success" != "true" ]; then
            echo "ACP probe did not succeed" >&2
            jq '.data.handshake_probe' "$OUTPUT_DIR/acp-diagnostic.json" >&2
            exit 1
        fi
    fi
}

validate_acp_diagnostic_all() {
    jq -c '.data[]' "$OUTPUT_DIR/acp-diagnostic.json" | while IFS= read -r item; do
        provider_id=$(printf '%s\n' "$item" | jq -r '.provider_id // "unknown"')
        readiness=$(printf '%s\n' "$item" | jq -r '.readiness // "unknown"')
        validate_readiness_value "$readiness" "ACP diagnostic for $provider_id" || exit 1
        if [ "$PROBE" -eq 1 ]; then
            attempted=$(printf '%s\n' "$item" | jq -r '.handshake_probe.attempted // false')
            success=$(printf '%s\n' "$item" | jq -r '.handshake_probe.success // false')
            if [ "$attempted" != "true" ] || [ "$success" != "true" ]; then
                echo "ACP probe did not succeed for $provider_id" >&2
                printf '%s\n' "$item" | jq '.handshake_probe' >&2
                exit 1
            fi
        fi
    done
}

main() {
    check_bridge_health
    bootstrap_auth
    fetch_agents
    fetch_acp_diagnostic
    echo "ACP smoke check completed."
    echo "Output directory: $OUTPUT_DIR"
}

main "$@"
