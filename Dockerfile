FROM golang:1.22 AS builder

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
RUN go build -v -a -o /azure-storage-usage-exporter cmd/main.go

FROM docker.io/debian:bookworm-slim

WORKDIR /tmp
RUN usermod -d /tmp nobody

COPY --from=builder /azure-storage-usage-exporter /

USER nobody
EXPOSE 8080

ENTRYPOINT ["/azure-storage-usage-exporter"]
