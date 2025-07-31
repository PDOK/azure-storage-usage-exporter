FROM golang:1.24 AS builder

WORKDIR /src
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd cmd
COPY internal internal

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=1
RUN go test -short ./...
RUN go build -v -a -o /azure-storage-usage-exporter github.com/PDOK/azure-storage-usage-exporter/cmd

FROM docker.io/debian:bookworm-slim

RUN set -eux && \
    apt-get update && \
    apt-get install -y libcurl4 curl openssl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /tmp
RUN usermod -d /tmp nobody

COPY --from=builder /azure-storage-usage-exporter /

USER nobody
EXPOSE 8080

ENTRYPOINT ["/azure-storage-usage-exporter"]
