#!/bin/sh
# Run with: sh scripts/test-c-sanitizers.sh [mix test arguments...]
set -eu

cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ]; then
  echo "This runner requires Linux, Clang, and its compiler-rt/LLVM tools." >&2
  exit 1
fi

export CC="${CLANG:-clang}"
target=$("$CC" -dumpmachine)
asan_runtime=$("$CC" -print-file-name="libclang_rt.asan-${target%%-*}.so")
if [ ! -f "$asan_runtime" ]; then
  echo "ASan runtime not found: $asan_runtime (install libclang-rt-dev)" >&2
  exit 1
fi
command -v llvm-symbolizer >/dev/null
export ASAN_SYMBOLIZER_PATH
ASAN_SYMBOLIZER_PATH=$(command -v llvm-symbolizer)

# Never replace the ordinary dev/test NIF with an instrumented library.
export MIX_ENV=test
export MIX_BUILD_PATH="$PWD/_build/c-sanitizers"
export SANITIZE=address,undefined
priv="$MIX_BUILD_PATH/lib/ekv/priv"
if [ -L "$priv" ]; then
  echo "Refusing to build through symlink: $priv" >&2
  exit 1
fi
mkdir -p "$priv"

# Force a fresh native build: make does not track compiler/flag changes.
# Build before preloading, so compiler subprocesses do not inherit ASan.
make -B MIX_APP_PATH="$MIX_BUILD_PATH/lib/ekv"

# Stock BEAM is not instrumented. Disable its optional pooling allocators so
# more NIF allocations reach malloc/free, where ASan can track their lifetime.
export ERL_FLAGS="${ERL_FLAGS:-} +Mea min"
export LD_PRELOAD="$asan_runtime${LD_PRELOAD:+:$LD_PRELOAD}"
# Whole-VM shutdown leaks are not an EKV leak oracle. This job checks invalid
# accesses/UB, not leak freedom; opt into LSan explicitly for investigations.
export ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=0}:halt_on_error=1"
export UBSAN_OPTIONS="${UBSAN_OPTIONS:-}:halt_on_error=1:print_stacktrace=1"

if [ "$#" -eq 0 ]; then
  set -- test/sqlite3_nif_test.exs test/ekv_test.exs test/wal_checkpointer_test.exs
fi
mix compile --warnings-as-errors
# Starting distribution at VM boot also starts epmd on a fresh machine.
exec elixir --name "c_sanitizers_$$@127.0.0.1" --cookie ekv_test \
  -S mix test --no-compile "$@"
