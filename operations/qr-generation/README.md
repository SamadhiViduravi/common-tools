# QR Code Generation Service

A simple HTTP service for generating QR codes built with Go.

## Features

- Generate QR codes from any text or URL
- Configurable QR code size
- RESTful API
- Health check endpoint
- Secure with request size limits and timeouts

## Prerequisites

- Go 1.21 or higher

## Installation

```bash
# Clone the repository
cd operations/qr-generation

# Install dependencies
go mod tidy

# Build the service
make build
```

## Running the Service

### Using Make

```bash
make run
```

### Using Go directly

```bash
go run cmd/api/main.go
```

### Using the binary

```bash
./bin/qr-api
```

The service will start on port 8080 by default.

## Configuration

Configure the service using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 8080 | Server port |
| `READ_TIMEOUT` | 5s | HTTP read timeout |
| `WRITE_TIMEOUT` | 10s | HTTP write timeout |
| `SHUTDOWN_TIMEOUT` | 5s | Graceful shutdown timeout |
| `MAX_BODY_SIZE` | 1048576 | Max request body size (1MB) |

Example:

```bash
PORT=8081 ./bin/qr-api
```

## API Endpoints

### Health Check

```bash
GET /health
```

Response:
```json
{
  "status": "ok"
}
```

### Generate QR Code

```bash
POST /generate?size={pixels}
```

**Query Parameters:**
- `size` (optional): QR code size in pixels (default: 256)

**Request Body:**
- Raw text or URL to encode

**Response:**
- PNG

**Examples:**

Generate a QR code for a URL:
```bash
curl -X POST "http://localhost:8080/generate?size=256" \
  -d "https://wso2.com" \
  --output qrcode.png
```

Generate a QR code for text:
```bash
curl -X POST "http://localhost:8080/generate?size=512" \
  -d "Hello World" \
  --output qrcode.png
```

## Development

### Build

```bash
make build
```

### Run tests

```bash
make test
```

### Clean build artifacts

```bash
make clean
```

### Update dependencies

```bash
make tidy
```

## Project Structure

```
.
├── cmd/
│   └── api/
│       └── main.go           # Application entry point
├── internal/
│   ├── config/
│   │   └── config.go         # Configuration management
│   ├── qr/
│   │   └── service.go        # QR code generation logic
│   └── transport/
│       └── http/
│           └── handler.go    # HTTP handlers
├── bin/                      # Build output (gitignored)
├── go.mod                    # Go module definition
├── go.sum                    # Go module checksums
├── Makefile                  # Build automation
└── README.md                 # This file
```

## License

See the LICENSE file in the repository root.
