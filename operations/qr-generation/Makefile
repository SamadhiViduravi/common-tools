.PHONY: run build clean test tidy

# Run the service
run:
	go run cmd/api/main.go

# Build the binary
build:
	mkdir -p bin
	go build -o bin/qr-api cmd/api/main.go

# Clean build artifacts and test files
clean:
	rm -rf bin/
	rm -f *.png *.log

# Run tests
test:
	go test -v ./...

# Tidy dependencies
tidy:
	go mod tidy
