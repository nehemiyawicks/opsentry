package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/config"
	"github.com/nehemiyawicks/opsentry/internal/httpapi"
	"github.com/nehemiyawicks/opsentry/internal/ingest"
	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/rpc"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

var (
	version = "dev"
	commit  = "none"
)

func main() {
	var cfgPath, addr, dbPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config file")
	flag.StringVar(&addr, "http.addr", ":8080", "http listen address")
	flag.StringVar(&dbPath, "db", "opsentry.db", "path to sqlite database")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logger.Info("opsentry starting", "version", version, "commit", commit)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	logger.Info("config loaded", "chains", len(cfg.Chains), "receivers", len(cfg.Receivers), "monitors", len(cfg.Monitors))

	store, err := storage.OpenSQLite(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer store.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()
	logger.Info("http listening", "addr", addr)

	var wg sync.WaitGroup
	for _, ch := range cfg.Chains {
		if len(ch.RPCs) == 0 {
			logger.Warn("chain has no rpcs, skipping", "chain", ch.ID)
			continue
		}
		client, err := rpc.Dial(ctx, ch.ID, ch.RPCs[0].URL)
		if err != nil {
			logger.Error("rpc dial failed, skipping chain", "chain", ch.ID, "err", err)
			continue
		}
		defer client.Close()

		rec := ingest.NewReconciler(ch.ID, 256, client)
		interval := time.Duration(ch.BlockTimeMs) * time.Millisecond
		if interval == 0 {
			interval = 2 * time.Second
		}
		chainLog := logger.With("chain", ch.ID)
		tracker := &ingest.HeadTracker{
			Chain:      ch.ID,
			Interval:   interval,
			Tag:        "latest",
			Reader:     client,
			Reconciler: rec,
			Store:      store,
			Log:        chainLog,
			OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
				chainLog.Info("canonical", "block", b.Number, "hash", fmt.Sprintf("%x", b.Hash[:6]))
			},
			OnReverted: func(_ context.Context, b pipeline.BlockRef) {
				chainLog.Warn("reverted", "block", b.Number, "hash", fmt.Sprintf("%x", b.Hash[:6]))
			},
		}
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if err := tracker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("tracker exited", "chain", id, "err", err)
			}
		}(ch.ID)
	}

	<-ctx.Done()
	logger.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	wg.Wait()
	logger.Info("opsentry stopped")
}
