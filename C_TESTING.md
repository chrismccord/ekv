# Checking the C NIF

The C checks complement `mix test` and the distributed/linearizability suites.
They do not prove transaction, replication, or CAS correctness.

## Strict warnings and static analysis

Install Clang and Erlang/OTP (including `erl_nif.h`), then run:

```sh
make c-check
```

This checks `c_src/ekv_sqlite3_nif.c` with `-Wall -Wextra -Wformat=2
-Wshadow -Werror`, then runs Clang Static Analyzer. Both compiler warnings
and analyzer findings fail the command. The vendored SQLite amalgamation
is excluded from these diagnostics. `CLANG=clang-18 make c-check` selects
a particular Clang; `ERTS_INCLUDE_DIR` can override the detected OTP headers.

The `c-check` CI job runs this on every PR and push to main. It does not
require fetching Mix dependencies or change the normal release build.

## ASan and UBSan (Linux)

On Ubuntu/Debian, install `clang libclang-rt-dev llvm`, plus the usual
Elixir/OTP, Make, and C build prerequisites:

```sh
mix deps.get
sh scripts/test-c-sanitizers.sh
```

The runner:

- Force-builds **both** the EKV wrapper and bundled SQLite with
  AddressSanitizer and UndefinedBehaviorSanitizer, debug symbols, and frame
  pointers. A report fails the run rather than allowing UB recovery.
- Uses `_build/c-sanitizers`, leaving normal dev/test NIFs untouched.
- Preloads the matching Clang ASan runtime before BEAM starts, including
  during Elixir compilation when the NIF's `on_load` can run.
- Uses LLVM's symbolizer for source locations.
- Passes `+Mea min` to BEAM to disable its optional pooling allocators.
  This routes more allocations through the system allocator ASan intercepts.
- Runs the focused NIF lifecycle tests, the main EKV tests (including store
  and single-node CAS coverage), and the WAL checkpointer tests.

Pass Mix test arguments to select different tests:

```sh
sh scripts/test-c-sanitizers.sh test/sqlite3_nif_test.exs --seed 0
```

The `c-sanitizers` CI job uses the same runner. On macOS, use a Linux
container/VM or CI; the runner intentionally does not guess at Darwin's
different runtime-loading requirements. Do not ship the instrumented NIF.

### What a clean run does not establish

Stock BEAM itself is not sanitizer-instrumented. NIF resource headers,
remaining VM allocators, and unexercised paths still limit coverage. The
lifecycle tests deliberately exercise process exit, GC, explicit cleanup,
cross-process resource ownership, binary/integer boundaries, concurrent
access, and rollback after partial failure. They are not allocation-failure
injection tests or a quantitative leak check.

LeakSanitizer is **disabled by default** (`detect_leaks=0`): whole-VM exit
reports from a stock BEAM are not a reliable EKV-only leak gate. For an
investigation, opt in with:

```sh
ASAN_OPTIONS=detect_leaks=1 sh scripts/test-c-sanitizers.sh test/sqlite3_nif_test.exs
```

Attribute findings before adding suppressions. A dedicated leak runner
(or Valgrind with an appropriately configured OTP build) is follow-up work,
not coverage provided by the current CI jobs.
