@echo off
cd consumer-service
if not exist go.mod (
  go mod init consumer-service
)
go get github.com/lib/pq
go get github.com/rabbitmq/amqp091-go
go mod tidy
cd ..
