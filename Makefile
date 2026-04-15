.PHONY: all clean build-logger

all: build-logger

build-logger:
	GOOS=linux GOARCH=arm64 go build -o bin/logger main.go

clean:
	rm -rf bin/