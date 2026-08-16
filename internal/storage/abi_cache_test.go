package storage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestABICacheRoundTrip(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	var addr [20]byte
	addr[19] = 0x42
	abiJSON := []byte(`[{"type":"event","name":"Ping","inputs":[]}]`)

	if _, ok, err := s.LoadCachedABI(ctx, 8453, addr); err != nil || ok {
		t.Fatalf("expected empty cache, got ok=%v err=%v", ok, err)
	}

	if err := s.SaveCachedABI(ctx, 8453, addr, abiJSON); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadCachedABI(ctx, 8453, addr)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if string(got) != string(abiJSON) {
		t.Fatalf("cache round-trip mismatch: got %s", got)
	}
}

func TestABICacheKeyedByChain(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	var addr [20]byte
	addr[19] = 0x42
	if err := s.SaveCachedABI(ctx, 8453, addr, []byte(`"base"`)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveCachedABI(ctx, 10, addr, []byte(`"op"`)); err != nil {
		t.Fatal(err)
	}

	base, _, _ := s.LoadCachedABI(ctx, 8453, addr)
	op, _, _ := s.LoadCachedABI(ctx, 10, addr)
	if string(base) != `"base"` || string(op) != `"op"` {
		t.Fatalf("same address on different chains should be cached separately: base=%s op=%s", base, op)
	}
}

func TestClearABICacheRemovesAll(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	var addr [20]byte
	addr[19] = 0x42
	_ = s.SaveCachedABI(ctx, 1, addr, []byte(`"a"`))
	_ = s.SaveCachedABI(ctx, 2, addr, []byte(`"b"`))

	n, err := s.ClearABICache(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("expected 2 rows cleared, got %d", n)
	}
	if _, ok, _ := s.LoadCachedABI(ctx, 1, addr); ok {
		t.Fatal("expected cache empty after clear")
	}
}

func TestABICacheUpsertReplaces(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	var addr [20]byte
	addr[19] = 0x42

	_ = s.SaveCachedABI(ctx, 1, addr, []byte("first"))
	_ = s.SaveCachedABI(ctx, 1, addr, []byte("second"))
	got, _, _ := s.LoadCachedABI(ctx, 1, addr)
	if string(got) != "second" {
		t.Fatalf("upsert should replace, got %s", got)
	}
}
