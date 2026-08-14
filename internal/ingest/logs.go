package ingest

import (
	"context"
	"log/slog"
	"math/big"
	"sort"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type LogClient interface {
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

type LogFetcher struct {
	Chain     string
	Client    LogClient
	Addresses []common.Address
	OnLog     func(context.Context, pipeline.Log)
	Log       *slog.Logger
}

func (f *LogFetcher) OnCanonical(ctx context.Context, b pipeline.BlockRef) {
	if f.OnLog == nil {
		return
	}
	q := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(b.Number),
		ToBlock:   new(big.Int).SetUint64(b.Number),
		Addresses: f.Addresses,
	}
	logs, err := f.Client.FilterLogs(ctx, q)
	if err != nil {
		f.logger().Warn("filter logs", "chain", f.Chain, "block", b.Number, "err", err)
		return
	}
	if len(logs) == 0 {
		return
	}
	sort.SliceStable(logs, func(i, j int) bool { return logs[i].Index < logs[j].Index })
	for _, lg := range logs {
		f.OnLog(ctx, toPipelineLog(f.Chain, b, lg))
	}
}

func (f *LogFetcher) logger() *slog.Logger {
	if f.Log != nil {
		return f.Log
	}
	return slog.Default()
}

func toPipelineLog(chain string, b pipeline.BlockRef, lg types.Log) pipeline.Log {
	var addr [20]byte
	copy(addr[:], lg.Address.Bytes())
	var txHash [32]byte
	copy(txHash[:], lg.TxHash.Bytes())
	topics := make([][32]byte, len(lg.Topics))
	for i, t := range lg.Topics {
		copy(topics[i][:], t.Bytes())
	}
	return pipeline.Log{
		Chain:    chain,
		Block:    b,
		TxHash:   txHash,
		TxIndex:  uint(lg.TxIndex),
		LogIndex: uint(lg.Index),
		Address:  addr,
		Topics:   topics,
		Data:     lg.Data,
	}
}
