package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/obs"
	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

type HeadFetcher interface {
	ChainReader
	HeadByTag(ctx context.Context, tag string) (pipeline.BlockRef, error)
}

type HeadTracker struct {
	Chain        string
	Interval     time.Duration
	Tag          string
	ConfirmDepth uint64
	Reader       HeadFetcher
	Reconciler   *Reconciler
	Store        storage.Store
	Log          *slog.Logger
	OnCanonical  func(context.Context, pipeline.BlockRef)
	OnReverted   func(context.Context, pipeline.BlockRef)

	mu      sync.Mutex
	pending []pipeline.BlockRef
}

func (t *HeadTracker) Run(ctx context.Context) error {
	if t.Tag == "" {
		t.Tag = "latest"
	}
	if t.Interval == 0 {
		t.Interval = 2 * time.Second
	}
	if err := t.seedFromStorage(ctx); err != nil {
		t.logger().Warn("seed from storage", "chain", t.Chain, "err", err)
	}
	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	if err := t.pollOnce(ctx); err != nil {
		t.logger().Warn("initial poll", "chain", t.Chain, "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := t.pollOnce(ctx); err != nil {
				t.logger().Warn("poll", "chain", t.Chain, "err", err)
			}
		}
	}
}

func (t *HeadTracker) logger() *slog.Logger {
	if t.Log != nil {
		return t.Log
	}
	return slog.Default()
}

func (t *HeadTracker) pollOnce(ctx context.Context) error {
	head, err := t.Reader.HeadByTag(ctx, t.Tag)
	if err != nil {
		return fmt.Errorf("head(%s): %w", t.Tag, err)
	}
	res, err := t.Reconciler.OnHead(ctx, head)
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}

	t.mu.Lock()
	t.pending = append(t.pending, res.Canonical...)
	for _, r := range res.Reverted {
		removed := false
		for i, p := range t.pending {
			if p.Number == r.Number && p.Hash == r.Hash {
				t.pending = append(t.pending[:i], t.pending[i+1:]...)
				removed = true
				break
			}
		}
		if !removed {
			obs.ReorgsSeen.WithLabelValues(t.Chain, "any").Inc()
			if t.OnReverted != nil {
				t.OnReverted(ctx, r)
			}
		}
	}

	threshold := uint64(0)
	if head.Number > t.ConfirmDepth {
		threshold = head.Number - t.ConfirmDepth
	}
	var emit, keep []pipeline.BlockRef
	for _, b := range t.pending {
		if b.Number <= threshold {
			emit = append(emit, b)
		} else {
			keep = append(keep, b)
		}
	}
	t.pending = keep
	t.mu.Unlock()

	sort.SliceStable(emit, func(i, j int) bool { return emit[i].Number < emit[j].Number })

	for _, b := range emit {
		if err := t.Store.RememberBlock(ctx, b); err != nil {
			t.logger().Warn("remember block", "chain", t.Chain, "block", b.Number, "err", err)
		}
		if t.OnCanonical != nil {
			t.OnCanonical(ctx, b)
		}
	}
	if len(emit) > 0 {
		newTip := emit[len(emit)-1]
		if err := t.Store.SaveCursor(ctx, storage.Cursor{Chain: t.Chain, Block: newTip}); err != nil {
			return fmt.Errorf("save cursor: %w", err)
		}
		obs.HeadLag.WithLabelValues(t.Chain, t.Tag).Set(float64(head.Number - newTip.Number))
	}
	return nil
}

func (t *HeadTracker) seedFromStorage(ctx context.Context) error {
	cur, ok, err := t.Store.LoadCursor(ctx, t.Chain)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	var minBlock uint64
	if cur.Block.Number > 256 {
		minBlock = cur.Block.Number - 256
	}
	blocks, err := t.Store.LoadRecentBlocks(ctx, t.Chain, minBlock)
	if err != nil {
		return err
	}
	t.Reconciler.LoadKnown(blocks)
	t.logger().Info("seeded reconciler from storage", "chain", t.Chain, "blocks", len(blocks), "tip", cur.Block.Number)
	return nil
}
