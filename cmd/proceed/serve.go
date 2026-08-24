package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"proceed/internal/api"
	"proceed/internal/controller"
	"proceed/internal/store"
)

const serveUsage = `usage:
  proceed serve [--data-dir <dir>] [--bind <addr>] [--config <file>]
`

func cmdServe(args []string, stdout, stderr io.Writer) int {
	flags, positional, err := parseCommonFlags(args)
	if err != nil || len(positional) != 0 {
		fmt.Fprint(stderr, serveUsage)
		return exitUsage
	}
	cfg, err := resolveConfig(flags)
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if len(cfg.Tokens) == 0 {
		fmt.Fprintln(stderr, "proceed serve: refusing to start without API tokens (deny by default); configure tokens in the config file")
		return exitUnclassified
	}
	if !cfg.LoopbackBind() {
		fmt.Fprintf(stderr, "proceed serve: non-loopback bind %q is an explicit operator setting\n", cfg.Bind)
	}

	st, err := store.Open(cfg.DataDir + "/proceed.db")
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	defer st.Close()

	serveCfg := controller.DefaultConfig()
	serveCfg.Mode = "serve"
	c, err := controller.New(st, serveCfg, buildPool())
	if err != nil {
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	if err := c.AcquireLease(context.Background()); err != nil {
		return printClassified(err, stderr)
	}

	listener, err := net.Listen("tcp", cfg.Bind)
	if err != nil {
		c.ReleaseLease()
		fmt.Fprintf(stderr, "proceed: %v\n", err)
		return exitUnclassified
	}
	server := &http.Server{Handler: api.NewServer(api.Deps{
		Store:      st,
		Controller: c,
		Config:     cfg,
	})}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.Serve(listener) }()
	fmt.Fprintf(stdout, "proceed serve listening on %s (store %s)\n", cfg.Bind, cfg.DataDir)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
			c.ReleaseLease()
			fmt.Fprintln(stdout, "proceed serve stopped")
			return exitOK
		case err := <-serverErr:
			c.ReleaseLease()
			if errors.Is(err, http.ErrServerClosed) {
				return exitOK
			}
			fmt.Fprintf(stderr, "proceed: %v\n", err)
			return exitUnclassified
		case <-ticker.C:
			if err := c.Heartbeat(); err != nil {
				_ = server.Close()
				c.ReleaseLease()
				return printClassified(err, stderr)
			}
			if err := c.RecoverAll(context.Background()); err != nil {
				fmt.Fprintf(stderr, "proceed: recovery scan: %v\n", err)
			}
		}
	}
}
