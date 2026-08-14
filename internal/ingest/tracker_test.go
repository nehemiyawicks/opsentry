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
