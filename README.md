# Sentinel Bridge Go

Sentinel Bridge is an intelligent, quorum-aware TCP proxy designed to sit between client applications and a Valkey/Redis Sentinel cluster.

It abstracts Sentinel failover logic away from the client. Clients connect to the proxy via a standard TCP connection, and the proxy autonomously discovers the current master database, routes traffic to it, and gracefully manages connections during cluster failovers.

## Architecture and Behavior
1. **Quorum Bootstrapping**: On startup, the proxy queries all configured Sentinels simultaneously. It will not begin accepting client traffic until a strict majority (quorum) of Sentinels agree on the current master address.
2. **Event Monitoring**: The proxy establishes persistent `Pub/Sub` connections (using the `valkey-go` library) to all Sentinels. It actively listens for `+switch-master` events.
3. **Data Plane**: Client connections are streamed directly to the current master using standard `io.CopyBuffer` with goroutines for high-throughput, low-latency communication.
4. **Failover Invalidation**: When a quorum of Sentinels report a `+switch-master` event (indicating a new master has been elected), the Quorum Coordinator updates the active backend route. The proxy immediately severs all active client connections routed to the old master, forcing clients to reconnect.
5. **Traffic Resumption**: As soon as clients reconnect, their new TCP streams are automatically routed to the newly promoted master.

## Configuration
The application is configured entirely via environment variables.
| Variable | Description | Default | Example |
| :--- | :--- | :--- | :--- |
| `PROXY_BIND_ADDR` | The `IP:PORT` the proxy listens on for client TCP traffic. | `0.0.0.0:6379` | `0.0.0.0:6379` |
| `HTTP_BIND_ADDR` | The `IP:PORT` for the HTTP server (Readiness probes and Metrics). | `0.0.0.0:8080` | `0.0.0.0:8080` |
| `MASTER_NAME` | The name of the Valkey/Redis master group monitored by Sentinel. | (Required) | `mymaster` |
| `SENTINEL_ADDRS` | A comma-separated list of Sentinel connection addresses. | (Required) | `10.0.0.1:26379,10.0.0.2:26379` |
| `LOG_LEVEL` | The log level for the application (`debug`, `info`, `warn`, `error`). | `info` | `debug` |

## Observability

### Logging
The application uses Go's standard `log/slog` library. Logs are output as structured JSON to `stdout`.
Log levels can be configured using the `LOG_LEVEL` environment variable. By default, the application logs at `INFO`.

### Metrics and Readiness
The proxy exposes an HTTP server on `HTTP_BIND_ADDR` with three endpoints:
* `/ready`: Returns HTTP 200 `OK` only when the proxy has successfully bootstrapped quorum and is actively routing traffic. Returns HTTP 503 otherwise.
* `/health`: A basic liveness probe.
* `/metrics`: Exposes Prometheus-formatted metrics.

**Available Prometheus Metrics:**
* `proxy_connections_total` (Counter): Total number of connections opened to the backend, labeled by `backend` IP.
* `proxy_active_connections` (Gauge): Current number of active connections to the backend, labeled by `backend` IP.
* `proxy_connections_closed_total` (Counter): Total number of closed connections, labeled by `backend` IP and `reason` (`client_disconnect`, `failover_severed`, `graceful_shutdown`).
* `proxy_backend_connection_errors_total` (Counter): Total number of failed connection attempts to backend, labeled by `backend` IP.

## Graceful Shutdown
The application intercepts `SIGINT`, `SIGTERM`, and `SIGQUIT` signals.
To support zero-downtime deployments in Kubernetes environments, the shutdown sequence executes as follows:
1. Receives termination signal.
2. Sleeps for 5 seconds to allow upstream LoadBalancers/kube-proxy to remove the pod IP.
3. Stops accepting new incoming TCP connections.
4. Waits indefinitely for all currently active streams to finish data transfer and close cleanly.

## Building and Running
The project requires Go to build. Dependencies are managed via Go modules.
```bash

# Build the binary
make build

# Run locally
make run

# A multistage `Containerfile` is provided to build the application into a minimal image.
make container TAG=myregistry/go-sentinel-bridge:latest
