.PHONY: build test run lint clean fmt vet

# Variables
BINARY_NAME=taskqueue
BUILD_DIR=bin
CMD_DIR=cmd/server

# Build the server binary
build:
	@echo "Building..."
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./$(CMD_DIR)

# Run the server
run: build
	@echo "Starting server..."
	@./$(BUILD_DIR)/$(BINARY_NAME)

# Run all tests with race detector
test:
	@echo "Running tests..."
	@go test -race -v ./...

# Run tests with coverage
test-cover:
	@echo "Running tests with coverage..."
	@go test -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	@echo "Linting..."
	@golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting..."
	@go fmt ./...

# Vet code
vet:
	@echo "Vetting..."
	@go vet ./...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem ./...

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR) coverage.out coverage.html
