package ingest

import (
	"context"
	"errors"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type fakeLogClient struct {
	logs  map[uint64][]types.Log
	err   error
	calls []struct{ from, to uint64 }
}

func (f *fakeLogClient) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	f.calls = append(f.calls, struct{ from, to uint64 }{q.FromBlock.Uint64(), q.ToBlock.Uint64()})
	if f.err != nil {
		return nil, f.err
	}
	var out []types.Log
	for n := q.FromBlock.Uint64(); n <= q.ToBlock.Uint64(); n++ {
		for _, l := range f.logs[n] {
			ll := l
			ll.BlockNumber = n
			out = append(out, ll)
		}
	}
	return out, nil
}

func mkLog(index uint, addr byte, topic0 byte) types.Log {
	return types.Log{
		Address: common.Address{addr},
		Topics:  []common.Hash{{topic0}},
		Data:    []byte{0xde, 0xad},
		Index:   index,
		TxHash:  common.Hash{0xaa},
	}
}

func TestLogFetcherEmitsInIndexOrder(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{
		100: {mkLog(2, 0x01, 0xaa), mkLog(0, 0x01, 0xbb), mkLog(1, 0x01, 0xcc)},
	}}
	var got []uint
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog: func(_ context.Context, l pipeline.Log) {
			got = append(got, l.LogIndex)
		},
	}
	f.OnCanonical(context.Background(), pipeline.BlockRef{Number: 100})
	if len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 2 {
		t.Fatalf("expected [0,1,2], got %v", got)
	}
}

func TestLogFetcherAttachesBlockAndChain(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{
		42: {mkLog(0, 0x01, 0xaa)},
	}}
	block := pipeline.BlockRef{Chain: "base", Number: 42, Hash: [32]byte{0xbb}}
	var out pipeline.Log
	f := &LogFetcher{
		Chain:     "base",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog:     func(_ context.Context, l pipeline.Log) { out = l },
	}
	f.OnCanonical(context.Background(), block)
	if out.Chain != "base" {
		t.Fatalf("expected chain=base, got %q", out.Chain)
	}
	if out.Block.Number != 42 || out.Block.Hash[0] != 0xbb {
		t.Fatalf("expected block 42 hash bb, got %+v", out.Block)
	}
	if out.Address[0] != 0x01 || out.Topics[0][0] != 0xaa {
		t.Fatalf("address/topic not copied: %+v", out)
	}
}

func TestLogFetcherNoLogsIsNoop(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{100: {}}}
	called := false
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog:     func(_ context.Context, _ pipeline.Log) { called = true },
	}
	f.OnCanonical(context.Background(), pipeline.BlockRef{Number: 100})
	if called {
		t.Fatal("OnLog should not be invoked when RPC returns zero logs")
	}
}

func TestLogFetcherRPCErrorIsLoggedNotFatal(t *testing.T) {
	fc := &fakeLogClient{err: errors.New("rpc down")}
	called := false
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog:     func(_ context.Context, _ pipeline.Log) { called = true },
	}
	f.OnCanonical(context.Background(), pipeline.BlockRef{Number: 100})
	if called {
		t.Fatal("OnLog should not be invoked on filter-logs error")
	}
}

func TestLogFetcherBatchCoalescesContiguousBlocks(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{
		100: {mkLog(0, 0x01, 0xaa)},
		101: {mkLog(0, 0x01, 0xbb)},
		102: {mkLog(0, 0x01, 0xcc)},
	}}
	var received []uint
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog: func(_ context.Context, l pipeline.Log) {
			received = append(received, uint(l.Block.Number))
		},
	}
	blocks := []pipeline.BlockRef{
		{Number: 100}, {Number: 101}, {Number: 102},
	}
	f.OnCanonicalBatch(context.Background(), blocks)
	if len(fc.calls) != 1 {
		t.Fatalf("expected 1 FilterLogs call for contiguous batch, got %d: %+v", len(fc.calls), fc.calls)
	}
	if fc.calls[0].from != 100 || fc.calls[0].to != 102 {
		t.Fatalf("expected range 100-102, got %+v", fc.calls[0])
	}
	if len(received) != 3 {
		t.Fatalf("expected 3 logs, got %d", len(received))
	}
}

func TestLogFetcherBatchSplitsAtGaps(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{
		100: {mkLog(0, 0x01, 0xaa)},
		101: {mkLog(0, 0x01, 0xbb)},
		105: {mkLog(0, 0x01, 0xcc)},
	}}
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		OnLog:     func(_ context.Context, _ pipeline.Log) {},
	}
	blocks := []pipeline.BlockRef{
		{Number: 100}, {Number: 101}, {Number: 105},
	}
	f.OnCanonicalBatch(context.Background(), blocks)
	if len(fc.calls) != 2 {
		t.Fatalf("expected 2 FilterLogs calls for non-contiguous batch, got %d: %+v", len(fc.calls), fc.calls)
	}
	if fc.calls[0].from != 100 || fc.calls[0].to != 101 {
		t.Fatalf("first call should be 100-101, got %+v", fc.calls[0])
	}
	if fc.calls[1].from != 105 || fc.calls[1].to != 105 {
		t.Fatalf("second call should be 105-105, got %+v", fc.calls[1])
	}
}

func TestLogFetcherBatchRespectsMaxChunkSize(t *testing.T) {
	fc := &fakeLogClient{logs: map[uint64][]types.Log{}}
	f := &LogFetcher{
		Chain:     "test",
		Client:    fc,
		Addresses: []common.Address{{0x01}},
		BatchSize: 3,
		OnLog:     func(_ context.Context, _ pipeline.Log) {},
	}
	var blocks []pipeline.BlockRef
	for n := uint64(100); n <= 108; n++ {
		blocks = append(blocks, pipeline.BlockRef{Number: n})
	}
	f.OnCanonicalBatch(context.Background(), blocks)
	if len(fc.calls) != 3 {
		t.Fatalf("expected 3 chunks (9 blocks / 3), got %d: %+v", len(fc.calls), fc.calls)
	}
	for i, c := range fc.calls {
		if c.to-c.from+1 > 3 {
			t.Errorf("chunk %d exceeds batch size: %+v", i, c)
		}
	}
}
