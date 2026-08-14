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
	logs map[uint64][]types.Log
	err  error
}

func (f *fakeLogClient) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.logs[q.FromBlock.Uint64()], nil
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
