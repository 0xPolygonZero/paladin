FROM rust:slim-bullseye as builder

RUN \
  mkdir -p paladin-opkind-derive/src  && touch paladin-opkind-derive/src/lib.rs && \
  mkdir -p paladin-core/src           && touch paladin-core/src/lib.rs && \
  mkdir -p ops/src                    && touch ops/src/lib.rs && \
  mkdir -p leader/src                 && echo "fn main() {println!(\"YO!\");}" > leader/src/main.rs

COPY Cargo.toml .
RUN sed -i "2s/.*/members = [\"paladin-core\", \"paladin-opkind-derive\", \"ops\", \"leader\"]/" Cargo.toml
COPY Cargo.lock .

COPY paladin-opkind-derive/Cargo.toml ./paladin-opkind-derive/Cargo.toml
COPY paladin-core/Cargo.toml ./paladin-core/Cargo.toml
COPY examples/hello-world-rabbitmq/ops/Cargo.toml ./ops/Cargo.toml
COPY examples/hello-world-rabbitmq/leader/Cargo.toml ./leader/Cargo.toml

RUN cargo build --release --bin leader 

COPY paladin-opkind-derive ./paladin-opkind-derive
COPY paladin-core ./paladin-core
COPY examples/hello-world-rabbitmq/ops ./ops
COPY examples/hello-world-rabbitmq/leader ./leader
RUN \
  touch paladin-opkind-derive/src/lib.rs && \
  touch paladin-core/src/lib.rs && \
  touch ops/src/lib.rs && \
  touch leader/src/main.rs

RUN cargo build --release --bin leader 

FROM debian:bullseye-slim
COPY --from=builder ./target/release/leader /usr/local/bin/leader
CMD ["leader"]
