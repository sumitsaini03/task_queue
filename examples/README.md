# TaskQueue Examples

This directory contains client examples and step-by-step walkthrough scripts interacting with the running `taskqueue` HTTP server.

## Running the Server First

Before executing any examples, start the server:

```bash
# Option 1: Native binary
go run ./cmd/server

# Option 2: Docker
docker-compose up
```

The server listens on `http://localhost:8080`.

---

## 1. Minimal Go Client

The Go client in `examples/client/main.go` uses standard library `net/http` to submit tasks, poll status, submit delayed tasks, and retrieve Prometheus metrics.

Run with:

```bash
go run ./examples/client
```

---

## 2. Curl / Bash Walkthrough

For Linux and macOS users:

```bash
chmod +x ./examples/walkthrough.sh
./examples/walkthrough.sh
```

---

## 3. PowerShell Walkthrough

For Windows users:

```powershell
.\examples\walkthrough.ps1
```
