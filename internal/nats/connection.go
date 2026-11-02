package nats

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/zimbatm/octoflow/internal/config"
)

// connectTimeout bounds the wait for a server that is still starting: the
// units start together after a switch and nats is ready a second later.
var connectTimeout = 30 * time.Second

// Connect opens a connection with the credentials in cfg, retrying for
// connectTimeout while the server refuses.
func Connect(cfg *config.NATSConfig) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.Timeout(cfg.ConnectionTimeout),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			slog.Error("NATS error", "error", err)
		}),
	}

	switch {
	case cfg.Username != "" && cfg.Password != "":
		opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
	case cfg.Token != "":
		opts = append(opts, nats.Token(cfg.Token))
	case cfg.NKey != "":
		opt, err := nats.NkeyOptionFromSeed(cfg.NKey)
		if err != nil {
			return nil, fmt.Errorf("invalid nkey seed: %w", err)
		}
		opts = append(opts, opt)
	}

	var nc *nats.Conn
	var err error
	for deadline := time.Now().Add(connectTimeout); ; {
		nc, err = nats.Connect(strings.Join(cfg.Servers, ","), opts...)
		if err == nil || !errors.Is(err, nats.ErrNoServers) || time.Now().After(deadline) {
			break
		}
		slog.Info("NATS not reachable yet; retrying", "error", err)
		time.Sleep(time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	return nc, nil
}

// ConnectJetStream connects, opens a JetStream context and ensures the
// events stream exists. The caller closes the connection.
func ConnectJetStream(cfg *config.NATSConfig) (*nats.Conn, nats.JetStreamContext, error) {
	nc, err := Connect(cfg)
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	if err := EnsureStream(js, cfg); err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}
