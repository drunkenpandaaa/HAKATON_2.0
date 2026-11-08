FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /inventory-api ./cmd/api

FROM alpine:3.20
WORKDIR /
COPY --from=builder /inventory-api /inventory-api
COPY migrations /migrations
EXPOSE 8080
ENTRYPOINT ["/inventory-api"]
