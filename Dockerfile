# syntax=docker/dockerfile:1.8

FROM golang:1.24 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://github.com/bufbuild/buf/releases/download/v1.64.0/buf-Linux-x86_64.tar.gz \
    | tar -xzf - -C /usr/local --strip-components=1 buf/bin/buf

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN buf generate buf.build/agynio/api --path agynio/api/threads/v1 --path agynio/api/notifications/v1

RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /out/threads ./cmd/threads

FROM gcr.io/distroless/base-debian12 AS runtime

WORKDIR /app

COPY --from=builder /out/threads /usr/local/bin/threads

USER nonroot:nonroot

ENTRYPOINT ["/usr/local/bin/threads"]
