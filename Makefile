build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o release/server cmd/iqdb/main.go

tests:
	go test -v -race

docker:
	docker build . --tag ravlio/iqdb:0.1.0


all:
	gen tests build
