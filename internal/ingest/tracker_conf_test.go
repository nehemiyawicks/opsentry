package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

type multiHead struct {
	*fakeReader
	mu    sync.Mutex
	heads map[string]pipeline.BlockRef
}

func (m *multiHead) HeadByTag(_ context.Context, tag string) (pipeline.BlockRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.heads[tag]
	if !ok {
		return pipeline.BlockRef{}, fmt.Errorf("unsupported tag %q", tag)
	}
	return b, nil
}

func (m *multiHead) setHead(tag string, b pipeline.BlockRef) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heads[tag] = b
}

func newConfStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "conf.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestConfirmationPollSeedsFirstAndEmitsOnAdvance(t *testing.T) {
	fr := newFakeReader()
	mh := &multiHead{
		fakeReader: fr,
		heads: map[string]pipeline.BlockRef{
			"safe": mkBlock(100, byte(100), byte(99)),
		},
	}
	store := newConfStore(t)
	ctx := context.Background()

	for n := uint64(90); n <= 110; n++ {
		if err := store.RememberBlock(ctx, pipeline.BlockRef{
			Chain: "test", Number: n,
			Hash:       [32]byte{byte(n)},
			ParentHash: [32]byte{byte(n - 1)},
		}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		emittedMu sync.Mutex
		emitted   [][]uint64
	)
	rec := NewReconciler("test", 256, fr)
	tt := &HeadTracker{
		Chain:      "test",
		Reader:     mh,
		Reconciler: rec,
		Store:      store,
		OnCanonicalSafe: func(_ context.Context, bs []pipeline.BlockRef) {
			nums := make([]uint64, len(bs))
			for i, b := range bs {
				nums[i] = b.Number
			}
			emittedMu.Lock()
			emitted = append(emitted, nums)
			emittedMu.Unlock()
		},
	}

	tt.pollConfirmationOnce(ctx, "safe", "safe", &tt.safeMu, &tt.safeTip, &tt.safeSeeded, tt.OnCanonicalSafe)
	emittedMu.Lock()
	if len(emitted) != 0 {
		t.Fatalf("first poll should seed only, got %v", emitted)
	}
	if !tt.safeSeeded || tt.safeTip != 100 {
		t.Fatalf("expected seeded=true tip=100, got seeded=%v tip=%d", tt.safeSeeded, tt.safeTip)
	}
	emittedMu.Unlock()

	mh.setHead("safe", mkBlock(103, byte(103), byte(102)))
	tt.pollConfirmationOnce(ctx, "safe", "safe", &tt.safeMu, &tt.safeTip, &tt.safeSeeded, tt.OnCanonicalSafe)

	emittedMu.Lock()
	defer emittedMu.Unlock()
	if len(emitted) != 1 {
		t.Fatalf("expected one emission batch, got %d", len(emitted))
	}
	got := emitted[0]
	if len(got) != 3 || got[0] != 101 || got[1] != 102 || got[2] != 103 {
		t.Fatalf("expected [101,102,103], got %v", got)
	}
	if tt.safeTip != 103 {
		t.Fatalf("expected tip to advance to 103, got %d", tt.safeTip)
	}
}

func TestConfirmationPollNoAdvanceSkips(t *testing.T) {
	fr := newFakeReader()
	mh := &multiHead{
		fakeReader: fr,
		heads: map[string]pipeline.BlockRef{
			"finalized": mkBlock(50, byte(50), byte(49)),
		},
	}
	store := newConfStore(t)
	ctx := context.Background()

	var emitted int
	rec := NewReconciler("test", 256, fr)
	tt := &HeadTracker{
		Chain:      "test",
		Reader:     mh,
		Reconciler: rec,
		Store:      store,
		OnCanonicalFinalized: func(_ context.Context, _ []pipeline.BlockRef) {
			emitted++
		},
	}

	tt.pollConfirmationOnce(ctx, "finalized", "finalized", &tt.finalizedMu, &tt.finalizedTip, &tt.finalizedSeeded, tt.OnCanonicalFinalized)
	tt.pollConfirmationOnce(ctx, "finalized", "finalized", &tt.finalizedMu, &tt.finalizedTip, &tt.finalizedSeeded, tt.OnCanonicalFinalized)
	tt.pollConfirmationOnce(ctx, "finalized", "finalized", &tt.finalizedMu, &tt.finalizedTip, &tt.finalizedSeeded, tt.OnCanonicalFinalized)

	if emitted != 0 {
		t.Fatalf("no advance means no emission, got %d", emitted)
	}
}

func TestConfirmationLoopRespectsContextCancel(t *testing.T) {
	fr := newFakeReader()
	mh := &multiHead{
		fakeReader: fr,
		heads:      map[string]pipeline.BlockRef{"safe": mkBlock(1, 1, 0)},
	}
	store := newConfStore(t)
	ctx, cancel := context.WithCancel(context.Background())

	tt := &HeadTracker{
		Chain:      "test",
		Reader:     mh,
		Reconciler: NewReconciler("test", 256, fr),
		Store:      store,
		OnCanonicalSafe: func(_ context.Context, _ []pipeline.BlockRef) {
		},
	}

	done := make(chan struct{})
	go func() {
		tt.runConfirmationLoop(ctx, "safe", 10*time.Millisecond, "safe", &tt.safeMu, &tt.safeTip, &tt.safeSeeded, tt.OnCanonicalSafe)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("confirmation loop did not exit within 500ms of context cancel")
	}
}
