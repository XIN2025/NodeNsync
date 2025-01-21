run: build
	@./bin/goredis --listenAddr :6379

build:
	@go build -o bin/goredis .
