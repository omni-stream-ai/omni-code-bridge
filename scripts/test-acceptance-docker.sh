#!/usr/bin/env bash
# Run the container acceptance fixtures against explicitly selected local images.
set -euo pipefail
cd "$(dirname "$0")/.."

resolve_image() {
  local reference="$1"
  if ! docker image inspect --format '{{.Id}}' "$reference"; then
    printf 'Missing local acceptance image: %s\nSet X11BOX_TEST_IMAGE and FLUTTER_TEST_IMAGE to existing image IDs or compatible tags.\n' "$reference" >&2
    return 1
  fi
}

export X11BOX_TEST_IMAGE
export FLUTTER_TEST_IMAGE
X11BOX_TEST_IMAGE="$(resolve_image "${X11BOX_TEST_IMAGE:-omni-x11box:acceptance}")"
FLUTTER_TEST_IMAGE="$(resolve_image "${FLUTTER_TEST_IMAGE:-omni-x11box:flutter-acceptance}")"
printf 'Base acceptance image: %s\nFlutter acceptance image: %s\n' "$X11BOX_TEST_IMAGE" "$FLUTTER_TEST_IMAGE"

for fixture in \
  real_image_probe_cache_and_refresh \
  real_derived_image_is_verified_and_reused \
  real_container_native_acceptance_without_browser \
  real_container_acceptance_lifecycle \
  real_flutter_desktop_acceptance; do
  cargo test "$fixture" -- --ignored --nocapture --test-threads=1
done
