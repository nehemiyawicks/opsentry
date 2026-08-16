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
	"github.com/nehemiyawicks/opsentry/internal/reads"
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
	var showVersion, checkOnly bool
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config file")
	flag.StringVar(&addr, "http.addr", ":8080", "http listen address")
	flag.StringVar(&dbPath, "db", "opsentry.db", "path to sqlite database")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.BoolVar(&checkOnly, "check", false, "validate config, compile rules, and exit")
	flag.Parse()

	if showVersion {
		fmt.Printf("opsentry %s (%s)\n", version, commit)
		return
	}

	if checkOnly {
		if err := checkConfig(cfgPath); err != nil {
			fmt.Fprintf(os.Stderr, "config check failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("config OK")
		return
	}

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

		addrs := monitorAddresses(cfg.Monitors, ch.ID)
		chainLog.Info("aggregated monitor addresses", "count", len(addrs))

		decoder := decode.NewDecoder()
		decoder.Log = chainLog
		decoder.Cache = decode.NewStoreABICache(store.LoadCachedABI, store.SaveCachedABI)
		for _, spec := range monitorSpecs(cfg.Monitors, ch) {
			spec.Storage = client
			if err := decoder.Register(ctx, spec); err != nil {
				chainLog.Warn("skip monitor: abi load failed", "monitor", spec.ID, "err", err)
			}
		}

		stateReaders := buildStateReaders(cfg.Monitors, ch.ID, client, chainLog)
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
				if r, ok := stateReaders[ev.MonitorID]; ok {
					r.Enrich(ctx, &ev)
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

		confirmDepth := uint64(ch.Confirmations.Fast)
		for _, m := range cfg.Monitors {
			if m.Chain != ch.ID {
				continue
			}
			if m.Confirmation != "" && m.Confirmation != "fast" {
				chainLog.Warn("monitor confirmation mode not yet supported, treating as fast",
					"monitor", m.ID, "requested", m.Confirmation,
					"note", "safe/finalized tag polling is a follow-up; chain.confirmations.fast applies to all monitors")
			}
		}
		tracker := &ingest.HeadTracker{
			Chain:        ch.ID,
			Interval:     interval,
			Tag:          "latest",
			ConfirmDepth: confirmDepth,
			Reader:       client,
			Reconciler:   rec,
			Store:        store,
			Log:          chainLog,
			OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
				chainLog.Info("canonical", "block", b.Number, "hash", fmt.Sprintf("%x", b.Hash[:6]))
			},
			OnCanonicalBatch: func(ctx context.Context, bs []pipeline.BlockRef) {
				logFetcher.OnCanonicalBatch(ctx, bs)
			},
			OnReverted: func(ctx context.Context, b pipeline.BlockRef) {
				chainLog.Warn("reverted", "block", b.Number, "hash", fmt.Sprintf("%x", b.Hash[:6]))
				stored, err := store.LoadAlertsAtBlock(ctx, ch.ID, b.Number, b.Hash)
				if err != nil {
					chainLog.Warn("load alerts for revert", "err", err)
					return
				}
				for _, sa := range stored {
					env := sa.Env
					if env == nil {
						env = map[string]any{}
					}
					env["kind"] = string(pipeline.AlertReverted)
					for _, rid := range sa.Receivers {
						if err := router.SendEnv(ctx, rid, env); err != nil {
							chainLog.Warn("notify revert", "receiver", rid, "err", err)
							continue
						}
						obs.AlertsSent.WithLabelValues(rid, string(pipeline.AlertReverted)).Inc()
					}
				}
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

func checkConfig(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if _, err := notify.BuildRouter(cfg.Receivers, nil); err != nil {
		return fmt.Errorf("receivers: %w", err)
	}
	for _, ch := range cfg.Chains {
		if _, err := rules.NewExprEvaluator(monitorRules(cfg.Monitors, ch.ID)); err != nil {
			return fmt.Errorf("chain %s rules: %w", ch.ID, err)
		}
	}
	fmt.Printf("chains=%d receivers=%d monitors=%d\n", len(cfg.Chains), len(cfg.Receivers), len(cfg.Monitors))
	return nil
}

func monitorAddresses(monitors []config.Monitor, chain string) []common.Address {
	seen := make(map[common.Address]struct{})
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
	}
	return out
}

func monitorSpecs(monitors []config.Monitor, ch config.Chain) []decode.MonitorSpec {
	var out []decode.MonitorSpec
	for _, m := range monitors {
		if m.Chain != ch.ID || m.Address == "" {
			continue
		}
		out = append(out, decode.MonitorSpec{
			ID:      m.ID,
			ChainID: ch.ChainID,
			Address: common.HexToAddress(m.Address),
			ABI:     m.ABI,
		})
	}
	return out
}

func buildStateReaders(monitors []config.Monitor, chain string, client *rpc.Client, logger *slog.Logger) map[string]*reads.Reader {
	out := make(map[string]*reads.Reader)
	for _, m := range monitors {
		if m.Chain != chain || len(m.Reads) == 0 {
			continue
		}
		defs := make([]reads.Def, 0, len(m.Reads))
		for _, r := range m.Reads {
			if r.Output == "" {
				logger.Warn("skipping state read without output type", "monitor", m.ID, "read", r.Name)
				continue
			}
			defs = append(defs, reads.Def{Name: r.Name, Method: r.Method, Output: r.Output})
		}
		if len(defs) == 0 {
			continue
		}
		out[m.ID] = &reads.Reader{
			MonitorID: m.ID,
			Address:   common.HexToAddress(m.Address),
			Defs:      defs,
			Client:    client,
			Log:       logger,
		}
		logger.Info("state reader configured", "monitor", m.ID, "reads", len(defs))
	}
	return out
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
