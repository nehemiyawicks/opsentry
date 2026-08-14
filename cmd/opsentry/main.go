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

	"github.com/ethereum/go-ethereum/common"

	"github.com/nehemiyawicks/opsentry/internal/alerts"
	"github.com/nehemiyawicks/opsentry/internal/config"
	"github.com/nehemiyawicks/opsentry/internal/decode"
	"github.com/nehemiyawicks/opsentry/internal/httpapi"
	"github.com/nehemiyawicks/opsentry/internal/ingest"
	"github.com/nehemiyawicks/opsentry/internal/notify"
	"github.com/nehemiyawicks/opsentry/internal/obs"
	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/rpc"
	"github.com/nehemiyawicks/opsentry/internal/rules"
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

	router, err := notify.BuildRouter(cfg.Receivers, logger)
	if err != nil {
		log.Fatalf("build receivers: %v", err)
	}
	logger.Info("receivers ready", "count", len(cfg.Receivers))
	alertMgr := &alerts.Manager{Store: store}

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

		addrs, addrToMonitor := monitorAddressesAndMap(cfg.Monitors, ch.ID)
		chainLog.Info("aggregated monitor addresses", "count", len(addrs))

		decoder, err := decode.NewERC20Decoder(addrToMonitor)
		if err != nil {
			logger.Error("decoder init failed, skipping chain", "chain", ch.ID, "err", err)
			continue
		}
		evaluator, err := rules.NewExprEvaluator(monitorRules(cfg.Monitors, ch.ID))
		if err != nil {
			logger.Error("rule compile failed, skipping chain", "chain", ch.ID, "err", err)
			continue
		}

		logFetcher := &ingest.LogFetcher{
			Chain:     ch.ID,
			Client:    client,
			Addresses: addrs,
			Log:       chainLog,
			OnLog: func(ctx context.Context, l pipeline.Log) {
				ev, err := decoder.Decode(ctx, l)
				if err != nil {
					chainLog.Debug("decode", "err", err)
					return
				}
				matches, err := evaluator.Eval(ctx, ev)
				if err != nil {
					chainLog.Warn("evaluate", "err", err)
					return
				}
				for _, m := range matches {
					alert, fired, err := alertMgr.Handle(ctx, m)
					if err != nil {
						chainLog.Warn("alert handle", "err", err)
						continue
					}
					if !fired {
						continue
					}
					for _, rid := range m.Receivers {
						if err := router.Send(ctx, rid, alert); err != nil {
							chainLog.Warn("notify", "receiver", rid, "err", err)
							continue
						}
						obs.AlertsSent.WithLabelValues(rid, string(alert.Kind)).Inc()
					}
				}
			},
		}

		tracker := &ingest.HeadTracker{
			Chain:      ch.ID,
			Interval:   interval,
			Tag:        "latest",
			Reader:     client,
			Reconciler: rec,
			Store:      store,
			Log:        chainLog,
			OnCanonical: func(ctx context.Context, b pipeline.BlockRef) {
				chainLog.Info("canonical", "block", b.Number, "hash", fmt.Sprintf("%x", b.Hash[:6]))
				logFetcher.OnCanonical(ctx, b)
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

func monitorAddressesAndMap(monitors []config.Monitor, chain string) ([]common.Address, map[string]string) {
	seen := make(map[common.Address]struct{})
	m2id := make(map[string]string)
	var out []common.Address
	for _, m := range monitors {
		if m.Chain != chain || m.Address == "" {
			continue
		}
		addr := common.HexToAddress(m.Address)
		if _, dup := seen[addr]; dup {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
		m2id[addr.Hex()] = m.ID
	}
	return out, m2id
}

func monitorRules(monitors []config.Monitor, chain string) []rules.MonitorRules {
	var out []rules.MonitorRules
	for _, m := range monitors {
		if m.Chain != chain {
			continue
		}
		mr := rules.MonitorRules{MonitorID: m.ID}
		for _, r := range m.Rules {
			mr.Rules = append(mr.Rules, rules.Rule{
				When:      r.When,
				Severity:  r.Severity,
				Receivers: r.Receivers,
			})
		}
		out = append(out, mr)
	}
	return out
}
