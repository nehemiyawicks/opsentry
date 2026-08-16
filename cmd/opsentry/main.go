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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
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
	var showVersion, checkOnly, refreshABI, listMonitors bool
	flag.StringVar(&cfgPath, "config", "config.yaml", "path to config file")
	flag.StringVar(&addr, "http.addr", ":8080", "http listen address")
	flag.StringVar(&dbPath, "db", "opsentry.db", "path to sqlite database")
	flag.BoolVar(&showVersion, "version", false, "print version and exit")
	flag.BoolVar(&checkOnly, "check", false, "validate config, compile rules, and exit")
	flag.BoolVar(&refreshABI, "refresh-abi", false, "clear cached ABIs at startup (forces refetch from Sourcify)")
	flag.BoolVar(&listMonitors, "list-monitors", false, "print configured monitors and exit")
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

	if listMonitors {
		if err := listMonitorsCmd(cfgPath); err != nil {
			fmt.Fprintf(os.Stderr, "list monitors failed: %v\n", err)
			os.Exit(1)
		}
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

	store, err := openStore(dbPath)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer store.Close()

	if refreshABI {
		n, err := store.ClearABICache(context.Background())
		if err != nil {
			log.Fatalf("clear abi cache: %v", err)
		}
		logger.Info("abi cache cleared", "rows", n)
	}

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

	evaluators := make(map[string]*atomic.Pointer[rules.ExprEvaluator])

	var wg sync.WaitGroup
	for _, ch := range cfg.Chains {
		if len(ch.RPCs) == 0 {
			logger.Warn("chain has no rpcs, skipping", "chain", ch.ID)
			continue
		}
		endpoints := make([]rpc.EndpointSpec, 0, len(ch.RPCs))
		for _, r := range ch.RPCs {
			endpoints = append(endpoints, rpc.EndpointSpec{URL: r.URL, Weight: r.Weight})
		}
		client, err := rpc.Dial(ctx, ch.ID, endpoints)
		if err != nil {
			logger.Error("rpc dial failed, skipping chain", "chain", ch.ID, "err", err)
			continue
		}
		defer client.Close()
		logger.Info("rpc pool ready", "chain", ch.ID, "endpoints", len(endpoints))

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
		if key := os.Getenv("ETHERSCAN_API_KEY"); key != "" {
			decoder.Etherscan = decode.NewEtherscanFetcher(key)
		}
		for _, spec := range monitorSpecs(cfg.Monitors, ch) {
			spec.Storage = client
			if err := decoder.Register(ctx, spec); err != nil {
				chainLog.Warn("skip monitor: abi load failed", "monitor", spec.ID, "err", err)
			}
		}

		stateReaders := buildStateReaders(cfg.Monitors, ch.ID, client, chainLog)
		initialEvaluator, err := rules.NewExprEvaluator(monitorRules(cfg.Monitors, ch.ID))
		if err != nil {
			logger.Error("rule compile failed, skipping chain", "chain", ch.ID, "err", err)
			continue
		}
		evalHolder := &atomic.Pointer[rules.ExprEvaluator]{}
		evalHolder.Store(initialEvaluator)
		evaluators[ch.ID] = evalHolder

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
				matches, err := evalHolder.Load().Eval(ctx, ev)
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

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hupCh:
				reloadRules(cfgPath, evaluators, logger)
			}
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	wg.Wait()
	logger.Info("opsentry stopped")
}

func reloadRules(cfgPath string, evaluators map[string]*atomic.Pointer[rules.ExprEvaluator], logger *slog.Logger) {
	newCfg, err := config.Load(cfgPath)
	if err != nil {
		logger.Warn("hot-reload: config load failed, keeping current rules", "err", err)
		return
	}
	reloaded := 0
	for chainID, holder := range evaluators {
		newEval, err := rules.NewExprEvaluator(monitorRules(newCfg.Monitors, chainID))
		if err != nil {
			logger.Warn("hot-reload: rule compile failed for chain, keeping current rules", "chain", chainID, "err", err)
			continue
		}
		holder.Store(newEval)
		reloaded++
	}
	logger.Info("hot-reload: rules reloaded", "chains", reloaded)
}

func listMonitorsCmd(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tCHAIN\tADDRESS\tABI\tCONFIRMATION\tREADS\tRULES")
	for _, m := range cfg.Monitors {
		addr := m.Address
		if len(addr) > 20 {
			addr = addr[:12] + "..."
		}
		conf := m.Confirmation
		if conf == "" {
			conf = "fast"
		}
		abi := m.ABI
		if strings.HasPrefix(strings.TrimSpace(abi), "[") {
			abi = fmt.Sprintf("inline (%dB)", len(abi))
		} else if abi == "" {
			abi = "-"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			m.ID, m.Chain, addr, abi, conf, len(m.Reads), len(m.Rules))
	}
	return w.Flush()
}

func openStore(dbPath string) (storage.Store, error) {
	if strings.HasPrefix(dbPath, "postgres://") || strings.HasPrefix(dbPath, "postgresql://") {
		return storage.OpenPostgres(context.Background(), dbPath)
	}
	return storage.OpenSQLite(dbPath)
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
