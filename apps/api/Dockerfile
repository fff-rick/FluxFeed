# syntax=docker/dockerfile:1

FROM golang:1.25.4-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fluxfeed-api ./cmd/feed
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fluxfeed-worker ./cmd/worker

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates ffmpeg tzdata

COPY --from=builder /out/fluxfeed-api ./fluxfeed-api
COPY --from=builder /out/fluxfeed-worker ./fluxfeed-worker
COPY configs ./configs

RUN mkdir -p uploads

EXPOSE 8080

CMD ["./fluxfeed-api"]
