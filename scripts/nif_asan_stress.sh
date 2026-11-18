#!/usr/bin/env bash
# Builds the SQLite NIF with AddressSanitizer and runs the NIF tests under it:
# the :nif_stress concurrency test plus the NIF safety and fetch_all suites.
# Run it before changing c_src/ekv_sqlite3_nif.c.
#
#   scripts/nif_asan_stress.sh [seconds] [concurrency]
#
# The ASan NIF is deleted on exit, so the next mix command builds a normal one.
set -euo pipefail
cd "$(dirname "$0")/.."

export MIX_ENV=test
export EKV_NIF_STRESS_SECS=${1:-30}
export EKV_NIF_STRESS_CONCURRENCY=${2:-64}
so=_build/test/lib/ekv/priv/ekv_sqlite3_nif.so
trap 'rm -f "$so"' EXIT

mix deps.get >/dev/null
rm -f "$so"
CFLAGS="-fsanitize=address -fno-omit-frame-pointer -g" LDFLAGS="-fsanitize=address" mix compile

# EKV_NIF_TESTS overrides the test files (space separated).
read -r -a tests <<< "${EKV_NIF_TESTS:-test/nif_stress_test.exs test/sqlite3_nif_safety_test.exs test/sqlite3_fetch_all_test.exs}"
export ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:verify_interceptors=0

case "$(uname -s)" in
Linux)
  LD_PRELOAD=$(${CC:-cc} -print-file-name=libasan.so) mix test --include nif_stress "${tests[@]}"
  ;;
Darwin)
  # mix, elixir and erl are /bin/sh scripts, and macOS drops DYLD_* variables
  # when it starts a protected binary like /bin/sh, so start erlexec directly.
  rt=$(ls "$(dirname "$(xcrun -f clang)")"/../lib/clang/*/lib/darwin/libclang_rt.asan_osx_dynamic.dylib | head -1)
  root=$(erl -noshell -eval 'io:format("~s", [code:root_dir()]), halt().')
  erts=$(erl -noshell -eval 'io:format("~s", [erlang:system_info(version)]), halt().')
  elib=$(elixir -e 'IO.write(Path.dirname(to_string(:code.lib_dir(:elixir))))')
  ROOTDIR=$root BINDIR=$root/erts-$erts/bin EMU=beam PROGNAME=erl DYLD_INSERT_LIBRARIES=$rt \
    "$root/erts-$erts/bin/erlexec" -noshell -elixir_root "$elib" -pa "$elib/elixir/ebin" \
    -s elixir start_cli -extra "$(command -v mix)" test --include nif_stress "${tests[@]}"
  ;;
esac
