package ingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type fakeReader struct {
	blocks map[uint64]pipeline.BlockRef
	fetches int
}

func newFakeReader() *fakeReader {
	return &fakeReader{blocks: make(map[uint64]pipeline.BlockRef)}
}

func (f *fakeReader) set(b pipeline.BlockRef) {
	f.blocks[b.Number] = b
}

func (f *fakeReader) BlockByNumber(_ context.Context, _ string, n uint64) (pipeline.BlockRef, error) {
	f.fetches++
	b, ok := f.blocks[n]
	if !ok {
		return pipeline.BlockRef{}, fmt.Errorf("no block at %d", n)
	}
	return b, nil
}

func mkBlock(n uint64, hash byte, parentHash byte) pipeline.BlockRef {
	return pipeline.BlockRef{
		Chain:      "test",
		Number:     n,
		Hash:       [32]byte{hash},
		ParentHash: [32]byte{parentHash},
	}
}

func numbers(bs []pipeline.BlockRef) []uint64 {
	out := make([]uint64, len(bs))
	for i, b := range bs {
		out[i] = b.Number
	}
	return out
}

func hashes(bs []pipeline.BlockRef) []byte {
	out := make([]byte, len(bs))
	for i, b := range bs {
		out[i] = b.Hash[0]
	}
	return out
}

func TestSeedEmitsFirstBlock(t *testing.T) {
	r := NewReconciler("test", 256, newFakeReader())
	res, err := r.OnHead(context.Background(), mkBlock(100, 0xaa, 0xa9))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Canonical) != 1 || res.Canonical[0].Number != 100 {
		t.Fatalf("expected [100] canonical, got %v", numbers(res.Canonical))
	}
	if len(res.Reverted) != 0 {
		t.Fatalf("expected no reverted, got %v", numbers(res.Reverted))
	}
}

func TestSequentialAppend(t *testing.T) {
	r := NewReconciler("test", 256, newFakeReader())
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	res, err := r.OnHead(ctx, mkBlock(101, 0xa1, 0xa0))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Canonical) != 1 || res.Canonical[0].Number != 101 {
		t.Fatalf("expected [101], got %v", numbers(res.Canonical))
	}
	if len(res.Reverted) != 0 {
		t.Fatalf("expected no reverted, got %v", numbers(res.Reverted))
	}
}

func TestDuplicateHeadIsNoOp(t *testing.T) {
	r := NewReconciler("test", 256, newFakeReader())
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	res, err := r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Canonical) != 0 || len(res.Reverted) != 0 {
		t.Fatalf("expected no-op, got canonical=%v reverted=%v", numbers(res.Canonical), numbers(res.Reverted))
	}
}

func TestGapForwardFetchesIntermediateBlocks(t *testing.T) {
	fr := newFakeReader()
	fr.set(mkBlock(101, 0xa1, 0xa0))
	fr.set(mkBlock(102, 0xa2, 0xa1))
	fr.set(mkBlock(103, 0xa3, 0xa2))
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	res, err := r.OnHead(ctx, mkBlock(104, 0xa4, 0xa3))
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(res.Canonical); len(got) != 4 || got[0] != 101 || got[3] != 104 {
		t.Fatalf("expected [101,102,103,104], got %v", got)
	}
	if len(res.Reverted) != 0 {
		t.Fatalf("expected no reverted, got %v", numbers(res.Reverted))
	}
	if fr.fetches != 3 {
		t.Fatalf("expected 3 fetches (101,102,103), got %d", fr.fetches)
	}
}

func TestSingleBlockReorgAtTip(t *testing.T) {
	fr := newFakeReader()
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	_, _ = r.OnHead(ctx, mkBlock(101, 0xa1, 0xa0))
	res, err := r.OnHead(ctx, mkBlock(101, 0xb1, 0xa0))
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(res.Canonical); len(got) != 1 || got[0] != 101 {
		t.Fatalf("expected canonical [101], got %v", got)
	}
	if got := hashes(res.Canonical); got[0] != 0xb1 {
		t.Fatalf("expected canonical hash 0xb1, got %x", got[0])
	}
	if got := numbers(res.Reverted); len(got) != 1 || got[0] != 101 {
		t.Fatalf("expected reverted [101], got %v", got)
	}
	if got := hashes(res.Reverted); got[0] != 0xa1 {
		t.Fatalf("expected reverted hash 0xa1, got %x", got[0])
	}
}

func TestDeepReorgWalksBackThreeBlocks(t *testing.T) {
	fr := newFakeReader()
	fr.set(mkBlock(101, 0xb1, 0xa0))
	fr.set(mkBlock(102, 0xb2, 0xb1))
	fr.set(mkBlock(103, 0xb3, 0xb2))
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	_, _ = r.OnHead(ctx, mkBlock(101, 0xa1, 0xa0))
	_, _ = r.OnHead(ctx, mkBlock(102, 0xa2, 0xa1))
	_, _ = r.OnHead(ctx, mkBlock(103, 0xa3, 0xa2))

	res, err := r.OnHead(ctx, mkBlock(104, 0xb4, 0xb3))
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(res.Canonical); len(got) != 4 {
		t.Fatalf("expected 4 canonical, got %v", got)
	}
	if got := hashes(res.Canonical); got[0] != 0xb1 || got[1] != 0xb2 || got[2] != 0xb3 || got[3] != 0xb4 {
		t.Fatalf("expected canonical hashes [b1,b2,b3,b4], got %x", got)
	}
	if got := numbers(res.Reverted); len(got) != 3 {
		t.Fatalf("expected 3 reverted, got %v", got)
	}
	if got := hashes(res.Reverted); got[0] != 0xa1 || got[1] != 0xa2 || got[2] != 0xa3 {
		t.Fatalf("expected reverted hashes [a1,a2,a3], got %x", got)
	}
}

func TestSiblingHeadLowerThanTip(t *testing.T) {
	fr := newFakeReader()
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	_, _ = r.OnHead(ctx, mkBlock(101, 0xa1, 0xa0))
	_, _ = r.OnHead(ctx, mkBlock(102, 0xa2, 0xa1))

	res, err := r.OnHead(ctx, mkBlock(101, 0xb1, 0xa0))
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(res.Canonical); len(got) != 1 || got[0] != 101 {
		t.Fatalf("expected canonical [101], got %v", got)
	}
	if got := numbers(res.Reverted); len(got) != 2 || got[0] != 101 || got[1] != 102 {
		t.Fatalf("expected reverted [101,102], got %v", got)
	}
	tip, ok := r.Tip()
	if !ok || tip.Number != 101 || tip.Hash[0] != 0xb1 {
		t.Fatalf("expected tip 101/b1, got %d/%x ok=%v", tip.Number, tip.Hash[0], ok)
	}
}

func TestReorgMidCatchupTriggersFullReorgFlow(t *testing.T) {
	fr := newFakeReader()
	fr.set(mkBlock(101, 0xb1, 0xa0))
	fr.set(mkBlock(102, 0xb2, 0xb1))
	fr.set(mkBlock(103, 0xb3, 0xb2))
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))
	_, _ = r.OnHead(ctx, mkBlock(101, 0xa1, 0xa0))

	res, err := r.OnHead(ctx, mkBlock(104, 0xb4, 0xb3))
	if err != nil {
		t.Fatal(err)
	}
	if got := numbers(res.Canonical); len(got) != 4 {
		t.Fatalf("expected 4 canonical, got %v", got)
	}
	if got := hashes(res.Canonical); got[0] != 0xb1 || got[3] != 0xb4 {
		t.Fatalf("expected canonical hashes to start b1 and end b4, got %x", got)
	}
	if got := numbers(res.Reverted); len(got) != 1 || got[0] != 101 {
		t.Fatalf("expected reverted [101], got %v", got)
	}
	if got := hashes(res.Reverted); got[0] != 0xa1 {
		t.Fatalf("expected reverted hash [a1], got %x", got)
	}
}

func TestChainShiftDuringWalkBackFails(t *testing.T) {
	fr := newFakeReader()
	fr.set(mkBlock(100, 0xff, 0x00))
	r := NewReconciler("test", 256, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, 0xa0, 0x00))

	_, err := r.OnHead(ctx, mkBlock(101, 0xb1, 0xb0))
	if err == nil {
		t.Fatal("expected error when RPC returns a block hash that doesn't match cursor.ParentHash")
	}
}

func TestPruneKeepsOnlyRecentBlocks(t *testing.T) {
	fr := newFakeReader()
	r := NewReconciler("test", 5, fr)
	ctx := context.Background()
	_, _ = r.OnHead(ctx, mkBlock(100, byte(100), byte(99)))
	for n := uint64(101); n <= 110; n++ {
		_, err := r.OnHead(ctx, mkBlock(n, byte(n), byte(n-1)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := r.known[100]; ok {
		t.Fatal("expected block 100 to be pruned")
	}
	if _, ok := r.known[104]; ok {
		t.Fatal("expected block 104 to be pruned (below tip - maxDepth = 110 - 5 = 105)")
	}
	if _, ok := r.known[105]; !ok {
		t.Fatal("expected block 105 to be retained")
	}
	if _, ok := r.known[110]; !ok {
		t.Fatal("expected tip 110 to be retained")
	}
}
