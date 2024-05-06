FROM golang:1.22 AS builder
ARG CGO_ENABLED=0
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src
COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY cmd/main.go cmd/main.go

RUN go test -short ./...
RUN go build -v -a -o /storage-usage-exporter cmd/main.go

FROM gcr.io/distroless/static:nonroot

WORKDIR /tmp
WORKDIR /

COPY --from=builder /storage-usage-exporter .

USER 65532:65532
EXPOSE 8080

ENTRYPOINT ["/storage-usage-exporter"]
