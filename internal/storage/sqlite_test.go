package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func newTempStore(t *testing.T) *SQLiteStore {
	t.Helper()
	dir := t.TempDir()
	s, err := OpenSQLite(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mkBlock(n uint64, hash, parent byte) pipeline.BlockRef {
	return pipeline.BlockRef{
		Chain:      "base",
		Number:     n,
		Hash:       [32]byte{hash},
		ParentHash: [32]byte{parent},
		Time:       time.Unix(1_700_000_000+int64(n), 0).UTC(),
	}
}

func TestCursorRoundTrip(t *testing.T) {
	s := newTempStore(t)
	ctx := context.Background()

	if _, ok, err := s.LoadCursor(ctx, "base"); err != nil || ok {
		t.Fatalf("expected no cursor, got ok=%v err=%v", ok, err)
	}

	b := mkBlock(100, 0xaa, 0xa9)
	if err := s.SaveCursor(ctx, Cursor{Chain: "base", Block: b}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadCursor(ctx, "base")
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Block.Number != 100 || got.Block.Hash[0] != 0xaa {
		t.Fatalf("cursor mismatch: %+v", got)
	}

	b2 := mkBlock(101, 0xab, 0xaa)
	if err := s.SaveCursor(ctx, Cursor{Chain: "base", Block: b2}); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.LoadCursor(ctx, "base")
	if got.Block.Number != 101 {
		t.Fatalf("expected cursor to advance to 101, got %d", got.Block.Number)
	}
}

func TestLoadCanonicalBlocksRange(t *testing.T) {
	s := newTempStore(t)
	ctx := context.Background()

	for n := uint64(100); n <= 110; n++ {
		if err := s.RememberBlock(ctx, mkBlock(n, byte(n), byte(n-1))); err != nil {
			t.Fatal(err)
		}
	}
	blocks, err := s.LoadCanonicalBlocksRange(ctx, "base", 103, 106)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 4 || blocks[0].Number != 103 || blocks[3].Number != 106 {
		t.Fatalf("expected [103..106], got %+v", blocks)
	}
	empty, err := s.LoadCanonicalBlocksRange(ctx, "base", 200, 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty range, got %d", len(empty))
	}
}

func TestRememberAndLoadRecent(t *testing.T) {
	s := newTempStore(t)
	ctx := context.Background()

	for n := uint64(100); n <= 110; n++ {
		if err := s.RememberBlock(ctx, mkBlock(n, byte(n), byte(n-1))); err != nil {
			t.Fatal(err)
		}
	}
	blocks, err := s.LoadRecentBlocks(ctx, "base", 105)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 6 || blocks[0].Number != 105 || blocks[5].Number != 110 {
		t.Fatalf("unexpected blocks: %+v", blocks)
	}

	replaced := mkBlock(105, 0xff, 0xfe)
	if err := s.RememberBlock(ctx, replaced); err != nil {
		t.Fatal(err)
	}
	blocks, _ = s.LoadRecentBlocks(ctx, "base", 105)
	if blocks[0].Hash[0] != 0xff {
		t.Fatalf("upsert did not replace hash, got %x", blocks[0].Hash[0])
	}
}
