# Releasing EKV

## Prerequisites

- Hex account with publish access
- GitHub repo with releases enabled (for precompiled NIF binaries)
- CI configured to run `mix elixir_make.precompile` on tag push (see below)

## Steps

1. **Bump `@version` in `mix.exs`**

2. **Tag and push**

   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. **Wait for CI to build precompiled binaries** for all targets and upload
   them to the GitHub release.

4. **Generate checksums**

   ```bash
   MIX_ENV=prod mix elixir_make.checksum --all --ignore-unavailable
   ```

   This creates `checksum-ekv.exs`.

5. **Publish to Hex**

   ```bash
   mix hex.publish
   ```

   Make sure `checksum-ekv.exs` is listed in the package `files`.

## CI precompile job (GitHub Actions)

```yaml
name: precompile

on:
  push:
    tags: ["v*"]

jobs:
  precompile:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]
    env:
      MIX_ENV: prod
    steps:
      - uses: actions/checkout@v4
      - uses: erlef/setup-beam@v1
        with:
          otp-version: "27"
          elixir-version: "1.18"
      - run: mix deps.get
      - run: |
          export ELIXIR_MAKE_CACHE_DIR=$(pwd)/cache
          mkdir -p "$ELIXIR_MAKE_CACHE_DIR"
          mix elixir_make.precompile
      - uses: softprops/action-gh-release@v2
        with:
          files: cache/*.tar.gz
```

## Verifying precompiled downloads

```bash
rm -rf _build/prod/lib/ekv
MIX_ENV=prod mix compile
mix test
```
