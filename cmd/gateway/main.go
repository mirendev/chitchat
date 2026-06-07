package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"miren.dev/chitchat/internal/auth"
	"miren.dev/chitchat/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	apiKey := os.Getenv("GATEWAY_API_KEY")
	if apiKey == "" {
		return errors.New("GATEWAY_API_KEY is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var verifier *auth.Verifier
	if multipassURL := os.Getenv("MULTIPASS_BASE_URL"); multipassURL != "" {
		allowedDomain := os.Getenv("ALLOWED_DOMAIN")
		v, err := auth.NewVerifier(ctx, multipassURL, allowedDomain)
		if err != nil {
			return err
		}
		verifier = v
		logger.Info("multipass auth enabled", "url", multipassURL, "domain", allowedDomain)
	}

	logger.Info("connecting to nats", "url", natsURL)

	var (
		nc  *nats.Conn
		err error
	)
	for {
		nc, err = nats.Connect(natsURL,
			nats.MaxReconnects(-1),
			nats.ReconnectWait(time.Second),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				if err != nil {
					logger.Warn("nats disconnected", "err", err)
				}
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				logger.Info("nats reconnected")
			}),
		)
		if err == nil {
			break
		}
		logger.Warn("nats not ready, retrying", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	if err := ensureDiscoveryStream(ctx, js, logger); err != nil {
		return err
	}

	authMW := auth.Middleware(apiKey, verifier, logger)
	srv := server.New(nc, js, authMW, logger)

	httpSrv := &http.Server{
		Addr:         ":8080",
		Handler:      srv.Handler(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // SSE needs unbounded writes
		IdleTimeout:  120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("gateway listening", "addr", httpSrv.Addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return httpSrv.Shutdown(shutdownCtx)
	}
}

// ensureDiscoveryStream provisions the DISCOVERY stream that backs the service
// discovery convention (see docs/SERVICE_DISCOVERY.md). Services heartbeat their
// descriptors to discovery.service.<service>.<instance>; max_msgs_per_subject=1
// keeps the latest descriptor per instance (last-known-state), and MaxAge ages
// out instances that stop heartbeating. CreateOrUpdateStream is idempotent, so
// this is safe on every startup and redeploy.
func ensureDiscoveryStream(ctx context.Context, js jetstream.JetStream, logger *slog.Logger) error {
	maxAge := 120 * time.Second
	if v := os.Getenv("DISCOVERY_MAX_AGE_SECONDS"); v != "" {
		secs, err := strconv.Atoi(v)
		if err != nil || secs <= 0 {
			return fmt.Errorf("invalid DISCOVERY_MAX_AGE_SECONDS %q: must be a positive integer", v)
		}
		maxAge = time.Duration(secs) * time.Second
	}

	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              "DISCOVERY",
		Subjects:          []string{"discovery.service.>"},
		Retention:         jetstream.LimitsPolicy,
		Discard:           jetstream.DiscardOld,
		MaxMsgsPerSubject: 1,
		MaxAge:            maxAge,
	})
	if err != nil {
		return fmt.Errorf("create DISCOVERY stream: %w", err)
	}
	logger.Info("DISCOVERY stream ready", "max_age", maxAge)
	return nil
}
