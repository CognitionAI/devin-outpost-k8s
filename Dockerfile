FROM rust:1.96-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release --bin devin-outposts-k8s

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /src/target/release/devin-outposts-k8s /usr/local/bin/devin-outposts-k8s
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/devin-outposts-k8s"]
