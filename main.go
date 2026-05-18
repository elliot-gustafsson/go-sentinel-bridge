package main

import (
	"context"
	"errors"
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

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024) // 32KB buffer
		return &b
	},
}

type SwitchMasterEvent struct {
	sentinelId int
	newAddr    string
}

type ProxyState struct {
	addr   string
	ctx    context.Context
	cancel context.CancelFunc
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
	for s := range strings.SplitSeq(sentinelAddrs, ",") {
		client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{strings.TrimSpace(s)}})
		if err != nil {
			slog.Error("error creating sentinel client", "error", err.Error())
			os.Exit(1)
		}
		defer client.Close()
		sentinels = append(sentinels, client)
	}

	if len(sentinels) == 0 {
		slog.Error("SENTINEL_ADDRS contains no valid endpoints")
		os.Exit(1)
	}

	quorumSize := (len(sentinels) / 2) + 1

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

	verifiedMaster, err := bootstrapQuorumMaster(signalCtx, sentinels, masterName, quorumSize)
	if err != nil {
		slog.Error("error running bootstrap", "error", err.Error())
		os.Exit(1)
	}

	slog.Info("bootstrap finished", "verified_master", verifiedMaster)

	ready.Store(true)

	eventChan := make(chan SwitchMasterEvent, 100)

	var statePointer atomic.Pointer[ProxyState]
	initialCtx, initialCancel := context.WithCancel(context.Background())
	statePointer.Store(&ProxyState{
		addr:   verifiedMaster,
		ctx:    initialCtx,
		cancel: initialCancel,
	})

	ctx, cancel := context.WithCancel(context.Background())

	for i, s := range sentinels {
		go runSentinelSubscriber(ctx, i, s, masterName, eventChan)
	}
	go runQuorumCoordinator(ctx, quorumSize, eventChan, &statePointer)

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
			select {
			case <-ctx.Done():
				break
			default:
				slog.Error("failed to accept connection", "error", err)
				continue
			}
			break
		}

		activeConnectionsWG.Go(func() {
			defer clientStream.Close()
			handleClientConnection(ctx, clientStream, &statePointer)
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

	listener.Close()
	ctxCancel()
}

func handleClientConnection(
	shutdownCtx context.Context,
	clientStream net.Conn,
	statePointer *atomic.Pointer[ProxyState],
) {

	currentState := statePointer.Load()
	if currentState == nil {
		slog.Error("unable to load current state")
		return
	}
	masterAddr := currentState.addr

	proxyConnectionsTotal.WithLabelValues(masterAddr).Inc()
	proxyActiveConnections.WithLabelValues(masterAddr).Inc()
	defer proxyActiveConnections.WithLabelValues(masterAddr).Dec()

	backendStream, err := net.DialTimeout("tcp", masterAddr, 3*time.Second)
	if err != nil {
		proxyBackendConnectionErrorsTotal.WithLabelValues(masterAddr).Inc()
		slog.Error("failed to connect to backend", "master_addr", masterAddr, "error", err)
		return
	}
	defer backendStream.Close()

	if tcpConn, ok := clientStream.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}
	if tcpConn, ok := backendStream.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	errChan := make(chan error, 2)

	// client -> backend
	go func() {
		buf := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(buf)
		_, err := io.CopyBuffer(backendStream, clientStream, *buf)
		errChan <- err
	}()

	// backend -> client
	go func() {
		buf := bufferPool.Get().(*[]byte)
		defer bufferPool.Put(buf)
		_, err := io.CopyBuffer(clientStream, backendStream, *buf)
		errChan <- err
	}()

	select {
	case <-errChan:
		proxyConnectionsClosedTotal.WithLabelValues(masterAddr, "client_disconnect").Inc()

	case <-currentState.ctx.Done():
		proxyConnectionsClosedTotal.WithLabelValues(masterAddr, "failover_severed").Inc()

	case <-shutdownCtx.Done():
		proxyConnectionsClosedTotal.WithLabelValues(masterAddr, "graceful_shutdown").Inc()
	}
}

func bootstrapQuorumMaster(ctx context.Context, sentinels []valkey.Client, masterName string, quorumSize int) (string, error) {
	for {

		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		results := make(chan string, len(sentinels))
		var wg sync.WaitGroup

		for _, s := range sentinels {
			wg.Go(func() {
				addr, err := bootstrapSingleMaster(ctx, s, masterName)
				if err == nil {
					results <- addr
				}
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

func bootstrapSingleMaster(ctx context.Context, sentinel valkey.Client, masterName string) (string, error) {

	slog.Info("querying sentinel")

	cmd := sentinel.B().Arbitrary("SENTINEL", "get-master-addr-by-name", masterName).Build()
	res, err := sentinel.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		return "", err
	}

	if len(res) == 2 {
		return net.JoinHostPort(res[0], res[1]), nil
	}
	return "", errors.New("invalid response format")
}

func runQuorumCoordinator(ctx context.Context, quorumSize int, eventChan <-chan SwitchMasterEvent, statePointer *atomic.Pointer[ProxyState]) {
	masterVotes := make(map[string]map[int]bool)

	for {

		select {
		case <-ctx.Done():
			return
		case event := <-eventChan:

			if masterVotes[event.newAddr] == nil {
				masterVotes[event.newAddr] = make(map[int]bool)
			}
			masterVotes[event.newAddr][event.sentinelId] = true

			currentState := statePointer.Load()

			if len(masterVotes[event.newAddr]) >= quorumSize && currentState.addr != event.newAddr {
				slog.Info("new master elected",
					"current_master", event.newAddr,
					"old_master", currentState.addr,
				)

				newCtx, newCancel := context.WithCancel(context.Background())
				newState := &ProxyState{
					addr:   event.newAddr,
					ctx:    newCtx,
					cancel: newCancel,
				}

				statePointer.Store(newState)
				currentState.cancel()

				masterVotes = make(map[string]map[int]bool)
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
