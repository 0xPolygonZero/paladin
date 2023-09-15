FROM rust:slim-bullseye as builder

RUN \
  mkdir -p opkind_derive/src  && touch opkind_derive/src/lib.rs && \
  mkdir -p paladin/src        && touch paladin/src/lib.rs && \
  mkdir -p ops/src            && touch ops/src/lib.rs && \
  mkdir -p orchestrator/src   && echo "fn main() {}" > orchestrator/src/main.rs

RUN echo "[workspace]\nmembers = [\"paladin\", \"opkind_derive\", \"ops\", \"orchestrator\"]\nresolver = \"2\"" > Cargo.toml
COPY Cargo.lock .

COPY opkind_derive/Cargo.toml ./opkind_derive/Cargo.toml
COPY paladin/Cargo.toml ./paladin/Cargo.toml
COPY ops/Cargo.toml ./ops/Cargo.toml
COPY orchestrator/Cargo.toml ./orchestrator/Cargo.toml

RUN cargo build --release --bin orchestrator 

COPY opkind_derive ./opkind_derive
COPY paladin ./paladin
COPY ops ./ops
COPY orchestrator ./orchestrator
RUN \
  touch opkind_derive/src/lib.rs && \
  touch paladin/src/lib.rs && \
  touch ops/src/lib.rs && \
  touch orchestrator/src/main.rs

RUN cargo build --release --bin orchestrator 

FROM debian:bullseye-slim
COPY --from=builder ./target/release/orchestrator /usr/local/bin/orchestrator
CMD ["orchestrator"]
