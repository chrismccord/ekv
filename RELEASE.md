# Releasing EKV

## Prerequisites

- Hex account with publish access
- GitHub repo with releases enabled (for precompiled NIF binaries)
- `gh` authenticated with access to push tags and create releases
- CI configured to run `.github/workflows/precompile.yml` on tag push

## Recommended path

From a clean checkout, run `./hex_release VERSION`. This bumps the version,
commits, tags and pushes it, waits for the full precompile workflow, generates
checksums, verifies the complete target matrix, and prompts to publish to Hex.
This command publishes a release; do not run it just to validate local changes.

## Manual steps

1. **Bump `@version` in `mix.exs`**

2. **Commit, tag and push the new version**

   ```bash
   git tag vX.Y.Z
   git push origin HEAD vX.Y.Z
   ```

3. **Wait for CI to build precompiled binaries** for all targets and upload
   them to the GitHub release. The final job generates and verifies
   `checksum.exs`, builds the Hex tarball as a packaging check, and uploads the
   checksum manifest to the GitHub release. It does not publish to Hex.

4. **Generate and verify checksums in the release checkout**

   ```bash
   MIX_ENV=prod mix deps.get
   MIX_ENV=prod mix elixir_make.checksum --all
   MIX_ENV=prod mix ekv.verify_checksums
   ```

   This creates `checksum.exs` (not `checksum-ekv.exs`). It is ignored by Git
   but included in the Hex package. Verification requires exactly one valid
   SHA-256 entry for every configured target/ABI at the current package version.
   Missing, partial, empty, and stale manifests fail closed.

   Do not rely on the download command's exit status or file existence alone:
   `elixir_make.checksum` can finish successfully with unavailable artifacts.

5. **Publish to Hex**

   ```bash
   MIX_ENV=docs mix hex.publish
   ```

   Both `mix hex.build` and `mix hex.publish` verify the manifest before
   packaging, so manually publishing cannot silently reuse an old release's
   checksums. `checksum.exs` is already listed in the package `files`.

## Supported build matrix

The compiler map and NIF versions in `mix.exs` define the expected artifacts:

- `x86_64-linux-gnu`, `aarch64-linux-gnu`: Ubuntu Jammy / glibc 2.35.
- `x86_64-linux-musl`, `aarch64-linux-musl`: Alpine 3.20 / musl 1.2.5.
- `x86_64-apple-darwin`, `aarch64-apple-darwin`: macOS 14.

Linux jobs build natively on x86-64 and ARM64 runners inside pinned containers.
Do not build glibc artifacts directly on `ubuntu-latest`: its libc may be newer
than the Debian/Ubuntu runtime image on a Fly Machine. Alpine needs distinct
musl binaries even on the same hardware.

All artifacts use NIF ABI 2.17, supported by OTP 26 and later. Building with
multiple OTP versions that expose the same NIF ABI overwrites the same filenames;
the precompile workflow intentionally builds each artifact only once.

## Verifying precompiled downloads

Each native build smoke-tests an actual EKV put/get after precompilation, with
`MAKE=false` during compilation so source fallback cannot hide a broken download
or checksum. The macOS runner builds both architectures and smoke-tests ARM64.

To verify locally, use a fresh build directory and cache:

```bash
MIX_ENV=prod mix ekv.verify_checksums
MIX_ENV=prod MIX_BUILD_PATH="$(mktemp -d)" ELIXIR_MAKE_CACHE_DIR="$(mktemp -d)" MAKE=false mix compile
mix test
```

The published 0.4.4 Hex package contains 0.3.0 checksums despite having 0.4.4
GitHub binaries. Hex permits replacing a package only within one hour of its
first publication ([publishing policy](https://hex.pm/docs/publish)). That window
has closed for 0.4.4, so release a new version through this workflow to repair
consumer installs. Updating GitHub assets cannot replace the checksum manifest
embedded in the Hex package. Consumers staying on 0.4.4 can use `EKV_BUILD=1`
with a C compiler and `make` installed to build from source.
