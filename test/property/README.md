# Property tests

StreamData is a **test-only** dependency. These are small, shrinkable models
against the real EKV API and SQLite NIF, not a simulated replacement store.
They run with the normal `mix test` suite.

```sh
mix test test/property
mix test test/property --seed 12345
```

By default StreamData checks 100 examples per property. For a longer run,
also raise ExUnit's per-property timeout: these examples reopen real databases.

```sh
MIX_ENV=test mix run -e 'Application.put_env(:stream_data, :max_runs, 1000); Mix.Task.run("test", ["test/property", "--seed", "12345", "--timeout", "300000"])'
```

## Current scope

| Model | Generated inputs | Assertions |
|---|---|---|
| Local LWW | Three keys, small values, put/delete/reopen commands | A reference map matches local reads, scans and keys after every command and reopen |
| Single-member CAS | One key, put/delete, symbolic current/absent/prior versions, rejected eventual writes, reopen | Success/conflict matches symbolic write identity; versions, local observations and barrier reads match the model |
| LWW replication and replay | Two valid origin streams, competing rows/timestamp ties, reordered/repeated single and batch deliveries, reopen, explicit repair | Per-entry winner flags, exact current rows, contiguous per-origin cursors, retained losing rows, idempotence and convergence of two real stores |
| CAS acceptor storage | Prepare/accept/promote rounds stopped before accept, after accept or after promote; stale ballots; reopen; expiry/delete metadata | Accepted-only state stays out of committed reads/scans/oplog; prepares recover exact accepted metadata; promotion preserves metadata and advances replay |

The fixed migration regression records the documented limitation: knowing CAS
ownership rejects **local** eventual writes, but a newer remote LWW row from a
stale member can still win on heal. It must not be changed into an expectation
of a partition-safe mode fence.

CAS and LWW have separate oracles. Local read-after-write assertions are not
claims that eventual reads on remote members are linearizable. The CAS API
model uses a healthy one-member quorum and does not model ambiguous outcomes.
The acceptor model tests storage primitives, not subscription delivery.

## Generation and shrinking rules

- Keep commands and values small. Boundary cases include zero extra commands,
  one-entry batches, delivery of sequence 2 before sequence 1, duplicates, and
  scan times just before/at/after expiry.
- Mandatory command cores retain successful operations and fault boundaries
  through shrinking. Assertions check actual applied/conflict/rejected/gap
  outcomes. Each generated CAS probe is paired with a successful write so
  histories cannot consist mostly of rejected calls.
- Version references are symbolic: `:current`, `:absent`, or `{:prior, distance}`
  into successful write history. The model compares write identities; opaque
  returned versions are used only as API inputs and observations. Shrinking
  never has to guess a valid timestamp or preserve a removed absolute index.
- Replication references resolve into valid per-origin streams. Repeated
  `(origin, sequence)` always means the same payload, not Byzantine input.
  A losing LWW delivery still contributes to replay progress.
- Every example and shrink gets a fresh directory under ExUnit's workspace
  `tmp/`. Reopen closes the actual database or supervisor before opening the
  same data again. Cleanup runs on assertion failures too.

When a property exposes a bug, preserve the minimized generated bindings as an
ordinary `test` alongside the property, using the same runner and literal
expected observations. Keep the regression independent of the random seed:
a seed alone is not a durable fixture across generator or dependency changes.
Do not replace the property with the fixture.

## Next slices

These properties do **not** provide a distributed scheduler or a built-in
linearizability checker. Continue using the distributed, pure Elixir
linearizability and Jepsen suites for those claims.

Follow-up models should cover subscription visibility through the replica
protocol, relays and replay/value identity separation, truncation and retention,
full/delta pagination with row/byte limits and terminal summaries, clock-driven
GC/quarantine, and duplicate logical membership IDs with loss/overlap. Reopen
here is a clean close/open boundary, not an OS crash or power-loss simulation.
