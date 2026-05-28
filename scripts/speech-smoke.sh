#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BRIDGE_URL=${BRIDGE_URL:-http://127.0.0.1:8787}
ASR_MODEL=${ASR_MODEL:-sensevoice-small-int8}
TTS_MODEL=${TTS_MODEL:-vits-melo-tts-zh-en}
REALTIME_ASR_MODEL=${REALTIME_ASR_MODEL:-streaming-paraformer-zh-en}
VAD_MODEL=${VAD_MODEL:-silero-vad}
VOICE=${VOICE:-0}
TEXT=${TEXT:-你好，欢迎使用本地语音测试。Hello from Omni Code Bridge.}
WITH_CALL_MODELS=0
WITH_REALTIME=0
SKIP_DOWNLOAD=0
KEEP_ARTIFACTS=0
AUTO_AUTH=1
OUTPUT_DIR=

usage() {
    cat <<'EOF'
Usage: scripts/speech-smoke.sh [options]

Runs an end-to-end local speech smoke test against a running bridge server:
1. ensures speech models are installed
2. binds speech profiles
3. calls /v1/audio/speech to synthesize a wav file
4. calls /v1/audio/transcriptions twice to verify profile fallback and explicit model use

Options:
  --bridge-url URL         Bridge base URL. Default: http://127.0.0.1:8787
  --client-id ID           Approved client id. If omitted, the script auto-provisions one.
  --token TOKEN            Approved bearer token. If omitted, the script auto-provisions one.
  --text TEXT              Input text for TTS.
  --voice ID               TTS speaker id. Default: 0
  --output-dir DIR         Keep artifacts in DIR.
  --keep-artifacts         Keep generated wav/json/txt outputs.
  --skip-download          Fail instead of downloading missing models.
  --with-call-models       Also install and bind realtime ASR and VAD profiles.
  --with-realtime          Run websocket realtime ASR smoke after batch TTS/ASR.
  --no-auto-auth           Require --client-id and --token instead of provisioning auth.
  -h, --help               Show this help.

Environment overrides:
  BRIDGE_URL
  BRIDGE_CLIENT_ID
  BRIDGE_TOKEN
  ASR_MODEL
  TTS_MODEL
  REALTIME_ASR_MODEL
  VAD_MODEL
  VOICE
  TEXT
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --bridge-url)
            BRIDGE_URL=$2
            shift 2
            ;;
        --client-id)
            BRIDGE_CLIENT_ID=$2
            shift 2
            ;;
        --token)
            BRIDGE_TOKEN=$2
            shift 2
            ;;
        --text)
            TEXT=$2
            shift 2
            ;;
        --voice)
            VOICE=$2
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
        --skip-download)
            SKIP_DOWNLOAD=1
            shift
            ;;
        --with-call-models)
            WITH_CALL_MODELS=1
            shift
            ;;
        --with-realtime)
            WITH_REALTIME=1
            WITH_CALL_MODELS=1
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
    OUTPUT_DIR=${TMPDIR:-/tmp}/omni-code-bridge-speech-smoke-$$
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
    curl -fsS "$BRIDGE_URL/health" >"$OUTPUT_DIR/health.json"
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

    BRIDGE_CLIENT_ID=speech-smoke-$(date +%s)
    request_body=$(jq -nc \
        --arg client_id "$BRIDGE_CLIENT_ID" \
        --arg device_name "speech-smoke" \
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

ensure_model_installed() {
    model_id=$1
    auth_curl "$BRIDGE_URL/speech/models" >"$OUTPUT_DIR/speech-models.json"
    installed=$(jq -r --arg id "$model_id" '.data[] | select(.id == $id) | .installed' "$OUTPUT_DIR/speech-models.json")

    if [ "$installed" = "true" ]; then
        echo "Model already installed: $model_id"
        return
    fi

    if [ "$SKIP_DOWNLOAD" -eq 1 ]; then
        echo "Model is not installed and --skip-download was set: $model_id" >&2
        exit 1
    fi

    request_body=$(jq -nc --arg model_id "$model_id" '{model_id:$model_id}')
    echo "Starting download for model: $model_id"
    auth_curl \
        -X POST \
        -H "Content-Type: application/json" \
        -d "$request_body" \
        "$BRIDGE_URL/speech/models/downloads" >"$OUTPUT_DIR/download-$model_id.json"

    task_id=$(jq -r '.data.task_id // empty' "$OUTPUT_DIR/download-$model_id.json")
    if [ -z "$task_id" ]; then
        echo "Failed to queue model download: $model_id" >&2
        exit 1
    fi

    while :; do
        auth_curl "$BRIDGE_URL/speech/models/downloads/$task_id" >"$OUTPUT_DIR/download-status-$model_id.json"
        status=$(jq -r '.data.status' "$OUTPUT_DIR/download-status-$model_id.json")
        progress=$(jq -r 'if .data.progress_bytes and .data.total_bytes then "\(.data.progress_bytes)/\(.data.total_bytes)" else (.data.progress_bytes // 0 | tostring) end' "$OUTPUT_DIR/download-status-$model_id.json")
        echo "Download $model_id: $status ($progress)"

        case "$status" in
            completed)
                return
                ;;
            failed)
                jq -r '.data.error // "unknown download failure"' "$OUTPUT_DIR/download-status-$model_id.json" >&2
                exit 1
                ;;
        esac
        sleep 2
    done
}

bind_profile() {
    profile=$1
    model_id=$2
    request_body=$(jq -nc --arg model_id "$model_id" '{model_id:$model_id}')
    echo "Binding profile $profile -> $model_id"
    auth_curl \
        -X PUT \
        -H "Content-Type: application/json" \
        -d "$request_body" \
        "$BRIDGE_URL/speech/profiles/$profile/model" >"$OUTPUT_DIR/profile-$profile.json"
}

verify_openai_models() {
    auth_curl "$BRIDGE_URL/v1/models" >"$OUTPUT_DIR/openai-models.json"
    if ! jq -e --arg id "$ASR_MODEL" '.data[] | select(.id == $id)' "$OUTPUT_DIR/openai-models.json" >/dev/null; then
        echo "ASR model is missing from /v1/models: $ASR_MODEL" >&2
        exit 1
    fi
    if ! jq -e --arg id "$TTS_MODEL" '.data[] | select(.id == $id)' "$OUTPUT_DIR/openai-models.json" >/dev/null; then
        echo "TTS model is missing from /v1/models: $TTS_MODEL" >&2
        exit 1
    fi
}

run_tts() {
    echo "Calling /v1/audio/speech with profile fallback"
    jq -nc \
        --arg input "$TEXT" \
        --arg voice "$VOICE" \
        '{input:$input,response_format:"wav",voice:$voice}' >"$OUTPUT_DIR/tts-request.json"

    auth_curl \
        -X POST \
        -H "Content-Type: application/json" \
        --data @"$OUTPUT_DIR/tts-request.json" \
        "$BRIDGE_URL/v1/audio/speech" >"$OUTPUT_DIR/tts.wav"
}

run_asr_profile_fallback() {
    echo "Calling /v1/audio/transcriptions with profile fallback"
    auth_curl \
        -X POST \
        -F "file=@$OUTPUT_DIR/tts.wav;type=audio/wav" \
        -F "response_format=verbose_json" \
        -F "timestamp_granularities[]=segment" \
        "$BRIDGE_URL/v1/audio/transcriptions" >"$OUTPUT_DIR/transcription-verbose.json"
}

run_asr_explicit_model() {
    echo "Calling /v1/audio/transcriptions with explicit ASR model"
    auth_curl \
        -X POST \
        -F "file=@$OUTPUT_DIR/tts.wav;type=audio/wav" \
        -F "model=$ASR_MODEL" \
        -F "response_format=text" \
        "$BRIDGE_URL/v1/audio/transcriptions" >"$OUTPUT_DIR/transcription.txt"
}

run_realtime_smoke() {
    need_cmd cargo
    echo "Calling /speech/realtime websocket smoke example"
    (
        cd "$ROOT_DIR"
        BRIDGE_URL=$BRIDGE_URL \
        BRIDGE_CLIENT_ID=$BRIDGE_CLIENT_ID \
        BRIDGE_TOKEN=$BRIDGE_TOKEN \
        WAV_PATH="$OUTPUT_DIR/tts.wav" \
        cargo run --quiet --example speech_realtime_smoke
    ) >"$OUTPUT_DIR/realtime-smoke.txt"
}

print_summary() {
    transcript_json=$(jq -r '.text // empty' "$OUTPUT_DIR/transcription-verbose.json")
    transcript_text=$(cat "$OUTPUT_DIR/transcription.txt")

    echo
    echo "Speech smoke test completed."
    echo "Bridge URL:     $BRIDGE_URL"
    echo "Client ID:      $BRIDGE_CLIENT_ID"
    echo "Batch ASR:      $ASR_MODEL"
    echo "TTS:            $TTS_MODEL"
    if [ "$WITH_CALL_MODELS" -eq 1 ]; then
        echo "Realtime ASR:   $REALTIME_ASR_MODEL"
        echo "VAD:            $VAD_MODEL"
    fi
    echo "TTS input:      $TEXT"
    echo "ASR verbose:    $transcript_json"
    echo "ASR text:       $transcript_text"
    if [ "$WITH_REALTIME" -eq 1 ]; then
        echo "Realtime log:   $OUTPUT_DIR/realtime-smoke.txt"
    fi
}

check_bridge_health
bootstrap_auth
ensure_model_installed "$ASR_MODEL"
ensure_model_installed "$TTS_MODEL"
bind_profile "asr.batch" "$ASR_MODEL"
bind_profile "tts.default" "$TTS_MODEL"

if [ "$WITH_CALL_MODELS" -eq 1 ]; then
    ensure_model_installed "$REALTIME_ASR_MODEL"
    ensure_model_installed "$VAD_MODEL"
    bind_profile "asr.realtime" "$REALTIME_ASR_MODEL"
    bind_profile "vad.default" "$VAD_MODEL"
fi

auth_curl "$BRIDGE_URL/speech" >"$OUTPUT_DIR/speech-status.json"
verify_openai_models
run_tts
run_asr_profile_fallback
run_asr_explicit_model
if [ "$WITH_REALTIME" -eq 1 ]; then
    run_realtime_smoke
fi
print_summary
