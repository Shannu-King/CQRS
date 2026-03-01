FROM golang:1.21-alpine AS builder

WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -o /command-service main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates curl wget
WORKDIR /root/
COPY --from=builder /command-service .

CMD ["./command-service"]
