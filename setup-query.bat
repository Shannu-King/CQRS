@echo off
cd query-service
if not exist go.mod (
  go mod init query-service
)
go get github.com/lib/pq
go get github.com/gorilla/mux
go mod tidy
cd ..
