FROM rust:slim-bullseye as builder

RUN \
  mkdir -p opkind_derive/src  && touch opkind_derive/src/lib.rs && \
  mkdir -p paladin/src        && touch paladin/src/lib.rs && \
  mkdir -p ops/src            && touch ops/src/lib.rs && \
  mkdir -p worker/src         && echo "fn main() {}" > worker/src/main.rs

RUN echo "[workspace]\nmembers = [\"paladin\", \"opkind_derive\", \"ops\", \"worker\"]\nresolver = \"2\"" > Cargo.toml
COPY Cargo.lock .

COPY opkind_derive/Cargo.toml ./opkind_derive/Cargo.toml
COPY paladin/Cargo.toml ./paladin/Cargo.toml
COPY examples/hello-world-rabbitmq/ops/Cargo.toml ./ops/Cargo.toml
COPY examples/hello-world-rabbitmq/worker/Cargo.toml ./worker/Cargo.toml

RUN cargo build --release --bin worker

COPY opkind_derive ./opkind_derive
COPY paladin ./paladin
COPY examples/hello-world-rabbitmq/ops ./ops
COPY examples/hello-world-rabbitmq/worker ./worker
RUN \
  touch opkind_derive/src/lib.rs && \
  touch paladin/src/lib.rs && \
  touch ops/src/lib.rs && \
  touch worker/src/main.rs

RUN cargo build --release --bin worker

FROM debian:bullseye-slim
COPY --from=builder ./target/release/worker /usr/local/bin/worker
CMD ["worker"]
