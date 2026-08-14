package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/config"
	"github.com/nehemiyawicks/opsentry/internal/httpapi"
	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

var (
	version = "dev"
	commit  = "none"
)

func main() {
	var cfgPath, addr string
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config file")
	flag.StringVar(&addr, "http.addr", ":8080", "http listen address")
	flag.Parse()

	log.Printf("opsentry %s (%s) starting", version, commit)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	log.Printf("loaded %d chains, %d receivers, %d monitors", len(cfg.Chains), len(cfg.Receivers), len(cfg.Monitors))

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
	log.Printf("http listening on %s", addr)

	runner := &pipeline.Runner{}
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("runner: %v", err)
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	log.Print("opsentry stopped")
}
