v1handler:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap handler/v1/main/main.go
	rm -f bootstrap.zip
	zip -9 -m bootstrap.zip bootstrap

v2handler:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -tags lambda.norpc -o bootstrap handler/v2/main/main.go
	rm -f bootstrap.zip
	zip -9 -m bootstrap.zip bootstrap

migrate:
	go run migrate/main.go
