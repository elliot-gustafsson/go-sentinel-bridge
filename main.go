package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valkey-io/valkey-go"
)

var (
	proxyConnectionsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_connections_total",
		Help: "Total number of connections opened to the backend",
	}, []string{"backend"})

	proxyConnectionsClosedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_connections_closed_total",
		Help: "Total number of connections closed",
	}, []string{"backend", "reason"})

	proxyBackendConnectionErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "proxy_backend_connection_errors_total",
		Help: "Total number of failed connection attempts to backend",
	}, []string{"backend"})

	proxyActiveConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "proxy_active_connections",
		Help: "Current number of active connections to the backend",
	}, []string{"backend"})
)

type SwitchMasterEvent struct {
	sentinelId int
	newAddr    string
}

type ProxyMetrics struct {
	connectionsTotal        prometheus.Counter
	activeConnections       prometheus.Gauge
	backendConnectionErrors prometheus.Counter
	closedGraceful          prometheus.Counter
	closedSevered           prometheus.Counter
	closedClientDisconnect  prometheus.Counter
}

type ProxyState struct {
	addr    string
	ctx     context.Context
	cancel  context.CancelFunc
	metrics ProxyMetrics
}

func NewProxyState(addr string) *ProxyState {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProxyState{
		addr:   addr,
		ctx:    ctx,
		cancel: cancel,
		metrics: ProxyMetrics{
			connectionsTotal:        proxyConnectionsTotal.WithLabelValues(addr),
			activeConnections:       proxyActiveConnections.WithLabelValues(addr),
			backendConnectionErrors: proxyBackendConnectionErrorsTotal.WithLabelValues(addr),
			closedGraceful:          proxyConnectionsClosedTotal.WithLabelValues(addr, "graceful_shutdown"),
			closedSevered:           proxyConnectionsClosedTotal.WithLabelValues(addr, "failover_severed"),
			closedClientDisconnect:  proxyConnectionsClosedTotal.WithLabelValues(addr, "client_disconnect"),
		},
	}
}

func main() {
	logLevel := slog.LevelInfo

	if lvl := os.Getenv("LOG_LEVEL"); lvl != "" {
		err := logLevel.UnmarshalText([]byte(lvl))
		if err != nil {
			slog.Error("error parsing LOG_LEVEL", "error", err.Error())
			os.Exit(1)
		}
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})))

	httpAddr := os.Getenv("HTTP_BIND_ADDR")
	if httpAddr == "" {
		httpAddr = "0.0.0.0:8080"
	}

	bindAddr := os.Getenv("PROXY_BIND_ADDR")
	if bindAddr == "" {
		bindAddr = "0.0.0.0:6379"
	}

	backendDialer := &net.Dialer{Timeout: 3 * time.Second}
	dialTimeout := os.Getenv("BACKEND_DIAL_TIMEOUT")
	if dialTimeout != "" {
		d, err := time.ParseDuration(dialTimeout)
		if err != nil {
			slog.Error("error parsing BACKEND_DIAL_TIMEOUT", "error", err.Error())
			os.Exit(1)
		}
		backendDialer.Timeout = d
	}

	masterName := os.Getenv("MASTER_NAME")
	if masterName == "" {
		slog.Error("MASTER_NAME environment variable is missing")
		os.Exit(1)
	}

	sentinelAddrs := os.Getenv("SENTINEL_ADDRS")
	if sentinelAddrs == "" {
		slog.Error("SENTINEL_ADDRS environment variable is missing")
		os.Exit(1)
	}

	var sentinels []valkey.Client
	var expectedSentinels int
	for s := range strings.SplitSeq(sentinelAddrs, ",") {
		expectedSentinels++
		addr := strings.TrimSpace(s)

		client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
		if err != nil {
			slog.Warn("failed to connect to sentinel", "sentinel_addr", addr, "error", err.Error())
			continue
		}
		defer client.Close()
		sentinels = append(sentinels, client)
	}

	if expectedSentinels == 0 {
		slog.Error("SENTINEL_ADDRS contains no valid endpoints")
		os.Exit(1)
	}

	quorumSize := (expectedSentinels / 2) + 1

	if len(sentinels) < quorumSize {
		slog.Error("not enough sentinels available to reach quorum",
			"available", len(sentinels),
			"required_quorum", quorumSize,
			"expected_total", expectedSentinels,
		)
		os.Exit(1)
	}

	reconcileInterval := 10 * time.Second
	if v := os.Getenv("RECONCILE_INTERVAL"); v != "" {
		i, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("error parsing RECONCILE_INTERVAL", "error", err.Error())
			os.Exit(1)
		}
		reconcileInterval = i
	}

	slog.Info("starting proxy",
		"bind_addr", bindAddr,
		"master_name", masterName,
		"quorum_size", quorumSize,
		"sentinels_count", len(sentinels),
	)

	var ready atomic.Bool

	go runHttpServer(httpAddr, &ready)

	signalCtx, signalCtxStop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	defer signalCtxStop()

	verifiedMaster, err := resolveMasterByQuorum(signalCtx, sentinels, masterName, quorumSize)
	if err != nil {
		slog.Error("error running bootstrap", "error", err.Error())
		os.Exit(1)
	}

	slog.Info("bootstrap finished", "verified_master", verifiedMaster)

	ready.Store(true)

	eventChan := make(chan SwitchMasterEvent, 100)

	var statePointer atomic.Pointer[ProxyState]
	statePointer.Store(NewProxyState(verifiedMaster))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i, s := range sentinels {
		go runSentinelSubscriber(ctx, i, s, masterName, eventChan)
	}
	go runQuorumCoordinator(ctx, quorumSize, eventChan, &statePointer)

	go runReconciliationLoop(ctx, sentinels, masterName, quorumSize, eventChan, &statePointer, reconcileInterval)

	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		slog.Error("failed to bind tcp listener", "error", err)
		os.Exit(1)
	}
	slog.Info("proxy listening on " + bindAddr)

	var activeConnectionsWG sync.WaitGroup

	go waitForShutdown(signalCtx, cancel, &ready, listener)

	for {
		clientStream, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			slog.Error("failed to accept connection", "error", err)
			continue
		}

		activeConnectionsWG.Go(func() {
			defer clientStream.Close()
			handleClientConnection(ctx, backendDialer, clientStream, &statePointer)
		})
	}

	slog.Info("waiting for active connections to drain...")
	activeConnectionsWG.Wait()
	slog.Info("shutting down, bye bye!")
}

func runHttpServer(addr string, ready *atomic.Bool) {
	router := http.NewServeMux()

	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	router.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)

		}
	})

	router.Handle("/metrics", promhttp.Handler())

	slog.Info("http server listening on " + addr)

	err := http.ListenAndServe(addr, router)
	if err != nil {
		slog.Error("http server error", "error", err.Error())
		return
	}
}

func waitForShutdown(signalCtx context.Context, ctxCancel func(), ready *atomic.Bool, listener net.Listener) {
	<-signalCtx.Done()

	slog.Info("received termination signal")
	ready.Store(false)

	slog.Info("sleeping for 5s...")
	time.Sleep(5 * time.Second)

	slog.Info("starting shutdown...")

	ctxCancel()
	listener.Close()
}

func handleClientConnection(
	shutdownCtx context.Context,
	dialer *net.Dialer,
	clientStream net.Conn,
	statePointer *atomic.Pointer[ProxyState],
) {

	currentState := statePointer.Load()
	if currentState == nil {
		slog.Error("unable to load current state")
		return
	}
	masterAddr := currentState.addr

	metrics := currentState.metrics
	metrics.connectionsTotal.Inc()
	metrics.activeConnections.Inc()
	defer metrics.activeConnections.Dec()

	backendStream, err := dialer.DialContext(shutdownCtx, "tcp", masterAddr)
	if err != nil {
		proxyBackendConnectionErrorsTotal.WithLabelValues(masterAddr).Inc()
		slog.Error("failed to connect to backend", "master_addr", masterAddr, "error", err)
		return
	}

	closeConns := func() {
		clientStream.Close()
		backendStream.Close()
	}
	defer closeConns()

	stopStateWatch := context.AfterFunc(currentState.ctx, closeConns)
	defer stopStateWatch()

	stopShutdownWatch := context.AfterFunc(shutdownCtx, closeConns)
	defer stopShutdownWatch()

	// NOTE: not using io.CopyBuffer due to *net.TCPConn implementing io.ReaderFrom, the buffer would just be unnecessary overhead

	// backend -> client
	go func() {
		_, _ = io.Copy(clientStream, backendStream)
		closeConns()
	}()

	// client -> backend
	_, _ = io.Copy(backendStream, clientStream)

	if shutdownCtx.Err() != nil {
		metrics.closedGraceful.Inc()
	} else if currentState.ctx.Err() != nil {
		metrics.closedSevered.Inc()
	} else {
		metrics.closedClientDisconnect.Inc()
	}
}

func resolveMasterByQuorum(ctx context.Context, sentinels []valkey.Client, masterName string, quorumSize int) (string, error) {
	for {

		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		results := make(chan string, len(sentinels))
		var wg sync.WaitGroup

		for i, s := range sentinels {
			wg.Go(func() {
				addr, err := querySentinelForMaster(ctx, s, masterName)
				if err != nil {
					slog.Error("error getting master address", "sentinel", i, "error", err.Error())
					return
				}
				results <- addr
			})
		}

		go func() {
			wg.Wait()
			close(results)
		}()

		votes := make(map[string]int)
		for addr := range results {
			votes[addr]++
			if votes[addr] >= quorumSize {
				return addr, nil
			}
		}

		slog.Error("failed to reach quorum, retrying in 2 seconds...")
		time.Sleep(2 * time.Second)
	}
}

func querySentinelForMaster(ctx context.Context, sentinel valkey.Client, masterName string) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	cmd := sentinel.B().Arbitrary("SENTINEL", "get-master-addr-by-name", masterName).Build()
	res, err := sentinel.Do(queryCtx, cmd).AsStrSlice()
	if err != nil {
		return "", err
	}

	if len(res) == 2 {
		return net.JoinHostPort(res[0], res[1]), nil
	}
	return "", fmt.Errorf("invalid response format: %s", res)
}

func runQuorumCoordinator(ctx context.Context, quorumSize int, eventChan <-chan SwitchMasterEvent, statePointer *atomic.Pointer[ProxyState]) {
	sentinelVotes := make(map[int]string)

	for {

		select {
		case <-ctx.Done():
			return
		case event := <-eventChan:
			currentState := statePointer.Load()

			// If there is no active election (len == 0) AND this is a vote for
			// the master we already have, throw it away.
			// This drops late echoes and ignores background polling noise.
			if len(sentinelVotes) == 0 && currentState != nil && currentState.addr == event.newAddr {
				continue
			}

			sentinelVotes[event.sentinelId] = event.newAddr

			voteCount := 0
			for _, addr := range sentinelVotes {
				if addr == event.newAddr {
					voteCount++
				}
			}

			if voteCount >= quorumSize && currentState != nil && currentState.addr != event.newAddr {
				slog.Info("new master elected",
					"current_master", event.newAddr,
					"old_master", currentState.addr,
				)

				newState := NewProxyState(event.newAddr)
				statePointer.Store(newState)

				currentState.cancel()

				clear(sentinelVotes)
			}
		}
	}
}

func runSentinelSubscriber(ctx context.Context, id int, sentinel valkey.Client, masterName string, eventChan chan<- SwitchMasterEvent) {
	for {

		sub := sentinel.B().Subscribe().Channel("+switch-master").Build()

		err := sentinel.Receive(ctx, sub, func(msg valkey.PubSubMessage) {
			if msg.Channel != "+switch-master" {
				return
			}

			parts := strings.Fields(msg.Message)
			if len(parts) >= 5 && parts[0] == masterName {
				newIP := parts[3]
				newPort := parts[4]
				newAddr := net.JoinHostPort(newIP, newPort)
				eventChan <- SwitchMasterEvent{
					sentinelId: id,
					newAddr:    newAddr,
				}
			}

		})

		if errors.Is(err, context.Canceled) {
			return
		}

		slog.Error("sentinel subscriber disconnected", "sentinel", id, "error", err.Error())
		time.Sleep(2 * time.Second)
	}

}

func runReconciliationLoop(
	ctx context.Context,
	sentinels []valkey.Client,
	masterName string,
	quorumSize int,
	eventChan chan<- SwitchMasterEvent,
	statePointer *atomic.Pointer[ProxyState],
	interval time.Duration,
) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			currentQuorumMaster, err := resolveMasterByQuorum(ctx, sentinels, masterName, quorumSize)
			if err != nil {
				slog.Info("background master reconciliation failed to reach quorum", "error", err)
				continue
			}
			currentState := statePointer.Load()
			if currentState != nil && currentState.addr != currentQuorumMaster {
				slog.Warn("background poller detected master drift (missed pub/sub event)",
					"current_proxy_target", currentState.addr,
					"actual_quorum_master", currentQuorumMaster,
				)

				for i := range sentinels {
					select {
					case eventChan <- SwitchMasterEvent{
						sentinelId: i,
						newAddr:    currentQuorumMaster,
					}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}
