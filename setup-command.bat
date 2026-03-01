@echo off
cd command-service
if not exist go.mod (
  go mod init command-service
)
go get github.com/lib/pq
go get github.com/rabbitmq/amqp091-go
go get github.com/gorilla/mux
go mod tidy
cd ..
