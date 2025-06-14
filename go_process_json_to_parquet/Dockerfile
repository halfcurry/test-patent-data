# Dockerfile

# Stage 1: Build the Go application
FROM golang:1.24-alpine AS builder

# Set working directory inside the builder stage
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker's build cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Build the Go application
# -o output_binary: Names the executable 'hupd_processor'
# -ldflags "-s -w": Strips debug symbols and DWARF tables for smaller binary size
# ./...: Builds all packages in the current directory and subdirectories
RUN CGO_ENABLED=0 GOOS=linux go build -o hupd_processor -ldflags "-s -w" ./...

# Stage 2: Create the final, minimal image
FROM alpine:latest

# Set the working directory to /app
WORKDIR /app

# Copy the compiled executable from the builder stage
COPY --from=builder /app/hupd_processor .

# Set the entrypoint for the container.
# This ensures the executable is run when the container starts.
ENTRYPOINT ["./hupd_processor"]
# The command can be left empty as the entrypoint defines the execution
# CMD []