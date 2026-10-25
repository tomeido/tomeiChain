ARG RUST_IMAGE=rust:1.96.1-bookworm
FROM ${RUST_IMAGE} AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY explorer ./explorer
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 tomei \
    && useradd --uid 10001 --gid tomei --no-create-home --home-dir /var/lib/tomei-chain tomei \
    && install -d -o tomei -g tomei /var/lib/tomei-chain
COPY --from=build /build/target/release/tomei-chain /usr/local/bin/tomei-chain
USER 10001:10001
WORKDIR /var/lib/tomei-chain
ENV TOMEI_DATA_DIR=/var/lib/tomei-chain \
    TOMEI_ADDR=0.0.0.0:8642 \
    TOMEI_MANTIS_ADDR=off \
    TOMEI_NODE_ROLE=follower
EXPOSE 8642
VOLUME ["/var/lib/tomei-chain"]
STOPSIGNAL SIGINT
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl --fail --silent --max-time 4 http://127.0.0.1:8642/health >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/tomei-chain"]
