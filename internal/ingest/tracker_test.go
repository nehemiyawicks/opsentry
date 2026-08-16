package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

type fakeHead struct {
	*fakeReader
	head pipeline.BlockRef
}

func (f *fakeHead) HeadByTag(_ context.Context, tag string) (pipeline.BlockRef, error) {
	if tag != "latest" {
		return pipeline.BlockRef{}, fmt.Errorf("unsupported tag %q", tag)
	}
	return f.head, nil
}

func newStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestHeadTrackerPollPersistsCursor(t *testing.T) {
	fr := newFakeReader()
	fh := &fakeHead{fakeReader: fr, head: mkBlock(100, 0xa0, 0x00)}
	rec := NewReconciler("test", 256, fr)
	store := newStore(t)

	var seen []uint64
	tt := &HeadTracker{
		Chain:      "test",
		Tag:        "latest",
		Interval:   50 * time.Millisecond,
		Reader:     fh,
		Reconciler: rec,
		Store:      store,
		OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
			seen = append(seen, b.Number)
		},
	}

	if err := tt.pollOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != 100 {
		t.Fatalf("expected [100], got %v", seen)
	}
	cur, ok, err := store.LoadCursor(context.Background(), "test")
	if err != nil || !ok {
		t.Fatalf("cursor: ok=%v err=%v", ok, err)
	}
	if cur.Block.Number != 100 || cur.Block.Hash[0] != 0xa0 {
		t.Fatalf("bad cursor: %+v", cur)
	}
}

func TestHeadTrackerReorgEmitsRevertedThenCanonical(t *testing.T) {
	fr := newFakeReader()
	fh := &fakeHead{fakeReader: fr, head: mkBlock(100, 0xa0, 0x00)}
	rec := NewReconciler("test", 256, fr)
	store := newStore(t)

	var canonical, reverted []uint64
	tt := &HeadTracker{
		Chain:      "test",
		Tag:        "latest",
		Reader:     fh,
		Reconciler: rec,
		Store:      store,
		OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
			canonical = append(canonical, b.Number)
		},
		OnReverted: func(_ context.Context, b pipeline.BlockRef) {
			reverted = append(reverted, b.Number)
		},
	}
	ctx := context.Background()

	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	fh.head = mkBlock(101, 0xa1, 0xa0)
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	fh.head = mkBlock(101, 0xb1, 0xa0)
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}

	if len(canonical) != 3 || canonical[0] != 100 || canonical[1] != 101 || canonical[2] != 101 {
		t.Fatalf("expected canonical [100,101,101], got %v", canonical)
	}
	if len(reverted) != 1 || reverted[0] != 101 {
		t.Fatalf("expected reverted [101], got %v", reverted)
	}
}

func TestConfirmDepthBuffersUntilThreshold(t *testing.T) {
	fr := newFakeReader()
	fh := &fakeHead{fakeReader: fr, head: mkBlock(100, 0xa0, 0x00)}
	rec := NewReconciler("test", 256, fr)
	store := newStore(t)
	ctx := context.Background()

	var emitted []uint64
	tt := &HeadTracker{
		Chain:        "test",
		Tag:          "latest",
		ConfirmDepth: 3,
		Reader:       fh,
		Reconciler:   rec,
		Store:        store,
		OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
			emitted = append(emitted, b.Number)
		},
	}

	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 0 {
		t.Fatalf("block 100 with head=100 depth=3 should be buffered, got emitted=%v", emitted)
	}

	fh.head = mkBlock(101, 0xa1, 0xa0)
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 0 {
		t.Fatalf("block 100 with head=101 depth=3 still buffered, got emitted=%v", emitted)
	}

	fh.head = mkBlock(103, 0xa3, 0xa2)
	fr.set(mkBlock(102, 0xa2, 0xa1))
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 1 || emitted[0] != 100 {
		t.Fatalf("expected [100] emitted at head=103 depth=3, got %v", emitted)
	}

	fh.head = mkBlock(104, 0xa4, 0xa3)
	fr.set(mkBlock(104, 0xa4, 0xa3))
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 2 || emitted[1] != 101 {
		t.Fatalf("expected [100,101] after head=104, got %v", emitted)
	}
}

func TestConfirmDepthDropsRevertedBufferedBlockSilently(t *testing.T) {
	fr := newFakeReader()
	fh := &fakeHead{fakeReader: fr, head: mkBlock(100, 0xa0, 0x00)}
	rec := NewReconciler("test", 256, fr)
	store := newStore(t)
	ctx := context.Background()

	var emitted, reverted []uint64
	tt := &HeadTracker{
		Chain:        "test",
		Tag:          "latest",
		ConfirmDepth: 5,
		Reader:       fh,
		Reconciler:   rec,
		Store:        store,
		OnCanonical:  func(_ context.Context, b pipeline.BlockRef) { emitted = append(emitted, b.Number) },
		OnReverted:   func(_ context.Context, b pipeline.BlockRef) { reverted = append(reverted, b.Number) },
	}

	_ = tt.pollOnce(ctx)
	fh.head = mkBlock(101, 0xa1, 0xa0)
	_ = tt.pollOnce(ctx)
	fh.head = mkBlock(101, 0xb1, 0xa0)
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(emitted) != 0 {
		t.Fatalf("nothing should have emitted yet at head=101 depth=5, got %v", emitted)
	}
	if len(reverted) != 0 {
		t.Fatalf("revert of buffered-but-unemitted block should be silent, got %v", reverted)
	}
}

func TestHeadTrackerSeedFromStorageRestoresKnown(t *testing.T) {
	fr := newFakeReader()
	fh := &fakeHead{fakeReader: fr, head: mkBlock(102, byte(102), byte(101))}
	store := newStore(t)
	ctx := context.Background()

	for n := uint64(98); n <= 101; n++ {
		if err := store.RememberBlock(ctx, mkBlock(n, byte(n), byte(n-1))); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.SaveCursor(ctx, storage.Cursor{Chain: "test", Block: mkBlock(101, byte(101), byte(100))}); err != nil {
		t.Fatal(err)
	}

	rec := NewReconciler("test", 256, fr)
	var canonical []uint64
	tt := &HeadTracker{
		Chain:      "test",
		Tag:        "latest",
		Reader:     fh,
		Reconciler: rec,
		Store:      store,
		OnCanonical: func(_ context.Context, b pipeline.BlockRef) {
			canonical = append(canonical, b.Number)
		},
	}
	if err := tt.seedFromStorage(ctx); err != nil {
		t.Fatal(err)
	}
	if tip, ok := rec.Tip(); !ok || tip.Number != 101 {
		t.Fatalf("expected tip 101, got %+v ok=%v", tip, ok)
	}
	if err := tt.pollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(canonical) != 1 || canonical[0] != 102 {
		t.Fatalf("expected only [102] after seeded restart, got %v", canonical)
	}
}
