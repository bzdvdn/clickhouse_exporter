# syntax=docker/dockerfile:1

FROM golang:1.26.11-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/clickhouse-exporter ./cmd/clickhouse-exporter

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/clickhouse-exporter /clickhouse-exporter
EXPOSE 9116
USER nonroot:nonroot
ENTRYPOINT ["/clickhouse-exporter"]
