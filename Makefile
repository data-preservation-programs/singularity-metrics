v1handler:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap handler/v1/main/main.go
	zip -9 -m bootstrap.zip bootstrap

lint:
	gofmt -s -w .
	golangci-lint run --fix
	staticcheck ./...

test:
	go test -race -coverprofile=coverage.out -coverpkg=./... ./...
