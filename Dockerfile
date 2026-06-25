# Build image for the outposts-operator binary.
# NOTE: this builds the *operator*, not the per-session devin worker image
# (that lightweight devin-CLI image is published separately; see docs/ARCHITECTURE.md).

FROM rust:1.96-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin outposts-operator

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/outposts-operator /usr/local/bin/outposts-operator
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/outposts-operator"]
