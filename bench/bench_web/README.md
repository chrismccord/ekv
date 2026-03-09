# BenchWeb

Phoenix LiveView orchestrator for distributed EKV CAS benchmarks.

## Local development

```bash
cd bench/bench_web
mix setup
mix phx.server
```

Open http://localhost:4000 and run CAS benchmarks from the UI.
The LiveView now streams run output in real time and lets you set
member/client roles per discovered node, choose `data_root`, toggle quick mode,
and select which benchmark scenarios to run.
Use the `Apply 9-member WAN preset` button to stamp in a mixed topology with
9 EKV members and the remaining nodes as clients.
Defaults are tuned for faster feedback on scenarios `4..9`.
`EKV.get(..., consistent: true)` in these scenarios is the barrier/linearizable
path (accept+commit), so quorum read latency reflects full consensus cost.

## Fly deployment (`ekv-bench`)

This app is built from the monorepo root because it depends on local path deps:

- `bench/bench_web -> ..`
- `bench -> ../` (EKV library)

From repository root:

```bash
bench/bench_web/deploy_fly.sh --app ekv-bench
```

`deploy_fly.sh` ensures `SECRET_KEY_BASE` and `RELEASE_COOKIE` exist before deploy.
If you prefer to set them manually:

```bash
fly secrets set SECRET_KEY_BASE="$(mix phx.gen.secret)" --app ekv-bench
fly secrets set RELEASE_COOKIE="<shared-erlang-cookie>" --app ekv-bench
```

Notes:

- `rel/env.sh.eex` sets `RELEASE_DISTRIBUTION=name` and defaults `RELEASE_NODE`
  to `bench_web@$FLY_PRIVATE_IP` on Fly.
- `rel/env.sh.eex` defaults `DNS_CLUSTER_QUERY` to `$FLY_APP_NAME.internal` on Fly
  unless you override it.
