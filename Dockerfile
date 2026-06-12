# syntax=docker/dockerfile:1

FROM node:22-alpine AS frontend-build
WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build

FROM golang:1.26-alpine AS backend-build
WORKDIR /app/backend

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY --from=frontend-build /app/frontend/build/ ./internal/server/build/

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ghidorah ./cmd/ghidorah

FROM alpine:latest AS runner
RUN apk add --no-cache ca-certificates

COPY --from=backend-build /out/ghidorah /usr/local/bin/ghidorah

EXPOSE 8042

ENTRYPOINT ["/usr/local/bin/ghidorah"]
