#!/bin/sh
set -eu

cd /home/junjie/code/omni-code-bridge
exec /home/junjie/.cargo/bin/cargo run --release
