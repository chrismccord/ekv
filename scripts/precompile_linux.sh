#!/bin/sh
# Run in a fresh Linux build container, with /work/cache mounted from the host.
set -eu

if command -v apk >/dev/null 2>&1; then
  apk add --no-cache build-base ca-certificates
  # Alpine names its native toolchain differently from the release triplet.
  # Give cc_precompiler explicit musl names; never label a glibc build as musl.
  ln -s "$(command -v gcc)" "/usr/local/bin/$(uname -m)-linux-musl-gcc"
  ln -s "$(command -v g++)" "/usr/local/bin/$(uname -m)-linux-musl-g++"
else
  apt-get update
  apt-get install -y --no-install-recommends build-essential ca-certificates
fi

mix local.hex --force
mix deps.get
mkdir -p "$ELIXIR_MAKE_CACHE_DIR"
mix elixir_make.precompile

# Precompile cleans priv. Disable make so a broken checksum/target cannot
# silently pass this smoke test by falling back to a source build.
MAKE=false mix compile --warnings-as-errors
mix run --no-compile scripts/precompile_smoke.exs
