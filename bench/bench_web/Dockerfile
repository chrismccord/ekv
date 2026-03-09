# syntax=docker/dockerfile:1

ARG ELIXIR_IMAGE=docker.io/elixir:1.19.2-otp-28
ARG RUNNER_IMAGE=docker.io/debian:bookworm-slim

FROM ${ELIXIR_IMAGE} AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential git curl \
  && rm -rf /var/lib/apt/lists/*

ENV MIX_ENV=prod
ENV EKV_BUILD=1
ENV MIX_BUILD_ROOT=/app/.mix_build
WORKDIR /app

RUN mix local.hex --force && mix local.rebar --force

# Copy the monorepo so local path deps (bench -> ekv) resolve during build.
COPY . .
RUN rm -rf /app/_build /app/deps /app/bench/_build /app/bench/deps /app/bench/bench_web/_build /app/bench/bench_web/deps
RUN rm -f /app/priv/ekv_sqlite3_nif.so

WORKDIR /app/bench/bench_web

RUN mix deps.get --only prod
RUN mix deps.compile
RUN mix assets.setup
RUN mix compile
RUN mix assets.deploy
RUN mix release

FROM ${RUNNER_IMAGE} AS final

RUN apt-get update \
  && apt-get install -y --no-install-recommends libstdc++6 openssl libncurses6 locales ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8
ENV MIX_ENV=prod

WORKDIR /app
RUN chown nobody /app

COPY --from=builder --chown=nobody:root /app/.mix_build/prod/rel/bench_web ./

USER nobody
CMD ["/app/bin/server"]
