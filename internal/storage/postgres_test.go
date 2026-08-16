//go:build integration

package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func openTestPostgres(t *testing.T) *PostgresStore {
	t.Helper()
	dsn := os.Getenv("OPSENTRY_POSTGRES_TEST_URL")
	if dsn == "" {
		t.Skip("OPSENTRY_POSTGRES_TEST_URL not set; skipping postgres integration tests")
	}
	s, err := OpenPostgres(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = s.pool.Exec(ctx, `TRUNCATE cursors, blocks, alerts, abi_cache`)
		_ = s.Close()
	})
	return s
}

func TestPostgresCursorRoundTrip(t *testing.T) {
	s := openTestPostgres(t)
	ctx := context.Background()

	if _, ok, err := s.LoadCursor(ctx, "base"); err != nil || ok {
		t.Fatalf("expected empty cursor: ok=%v err=%v", ok, err)
	}
	b := pipeline.BlockRef{
		Chain:      "base",
		Number:     100,
		Hash:       [32]byte{0xaa},
		ParentHash: [32]byte{0xa9},
		Time:       time.Unix(1_700_000_000, 0).UTC(),
	}
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
}

func TestPostgresBlockRoundTrip(t *testing.T) {
	s := openTestPostgres(t)
	ctx := context.Background()

	for n := uint64(100); n <= 105; n++ {
		b := pipeline.BlockRef{
			Chain: "base", Number: n,
			Hash:       [32]byte{byte(n)},
			ParentHash: [32]byte{byte(n - 1)},
			Time:       time.Unix(int64(n), 0).UTC(),
		}
		if err := s.RememberBlock(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	blocks, err := s.LoadRecentBlocks(ctx, "base", 103)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 3 || blocks[0].Number != 103 {
		t.Fatalf("recent blocks: %+v", blocks)
	}
}

func TestPostgresABICacheRoundTrip(t *testing.T) {
	s := openTestPostgres(t)
	ctx := context.Background()

	var addr [20]byte
	addr[19] = 0x42
	abiJSON := []byte(`[{"type":"event","name":"Ping","inputs":[]}]`)

	if err := s.SaveCachedABI(ctx, 8453, addr, abiJSON); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadCachedABI(ctx, 8453, addr)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if string(got) != string(abiJSON) {
		t.Fatalf("mismatch: %s vs %s", got, abiJSON)
	}
	n, err := s.ClearABICache(ctx)
	if err != nil || n != 1 {
		t.Fatalf("clear: n=%d err=%v", n, err)
	}
}

// keep filepath import used for consistency with sqlite_test
var _ = filepath.Join
