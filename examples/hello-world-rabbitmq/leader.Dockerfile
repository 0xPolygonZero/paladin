FROM rust:slim-bullseye as builder

RUN \
  mkdir -p opkind_derive/src  && touch opkind_derive/src/lib.rs && \
  mkdir -p paladin/src        && touch paladin/src/lib.rs && \
  mkdir -p ops/src            && touch ops/src/lib.rs && \
  mkdir -p leader/src         && echo "fn main() {println!(\"YO!\");}" > leader/src/main.rs

RUN echo "[workspace]\nmembers = [\"paladin\", \"opkind_derive\", \"ops\", \"leader\"]\nresolver = \"2\"" > Cargo.toml
COPY Cargo.lock .

COPY opkind_derive/Cargo.toml ./opkind_derive/Cargo.toml
COPY paladin/Cargo.toml ./paladin/Cargo.toml
COPY examples/hello-world-rabbitmq/ops/Cargo.toml ./ops/Cargo.toml
COPY examples/hello-world-rabbitmq/leader/Cargo.toml ./leader/Cargo.toml

RUN cargo build --release --bin leader 

COPY opkind_derive ./opkind_derive
COPY paladin ./paladin
COPY examples/hello-world-rabbitmq/ops ./ops
COPY examples/hello-world-rabbitmq/leader ./leader
RUN \
  touch opkind_derive/src/lib.rs && \
  touch paladin/src/lib.rs && \
  touch ops/src/lib.rs && \
  touch leader/src/main.rs

RUN cargo build --release --bin leader 

FROM debian:bullseye-slim
COPY --from=builder ./target/release/leader /usr/local/bin/leader
CMD ["leader"]
