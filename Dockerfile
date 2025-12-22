FROM golang:1.24.2-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    pkg-config \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

ENV CGO_ENABLED=1
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build \
    -ldflags="-s -w" \
    -o main \
    ./cmd/synckafka


FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    librdkafka1 \
    ca-certificates \
    tzdata \
 && ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
 && echo "Asia/Shanghai" > /etc/timezone \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/internal/config ./config

ENV CONFIG_PATH=/app/config

CMD ["sleep", "infinity"]
