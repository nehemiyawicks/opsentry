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

const defaultLogsBatchSize = 1000

type LogClient interface {
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
}

type LogFetcher struct {
	Chain     string
	Client    LogClient
	Addresses []common.Address
	BatchSize int
	OnLog     func(context.Context, pipeline.Log)
	Log       *slog.Logger
}

func (f *LogFetcher) OnCanonical(ctx context.Context, b pipeline.BlockRef) {
	f.OnCanonicalBatch(ctx, []pipeline.BlockRef{b})
}

func (f *LogFetcher) OnCanonicalBatch(ctx context.Context, blocks []pipeline.BlockRef) {
	if f.OnLog == nil || len(blocks) == 0 {
		return
	}
	byNumber := make(map[uint64]pipeline.BlockRef, len(blocks))
	nums := make([]uint64, 0, len(blocks))
	for _, b := range blocks {
		byNumber[b.Number] = b
		nums = append(nums, b.Number)
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })

	batchSize := f.BatchSize
	if batchSize <= 0 {
		batchSize = defaultLogsBatchSize
	}

	for i := 0; i < len(nums); {
		start := i
		end := i
		for end+1 < len(nums) && nums[end+1] == nums[end]+1 && (end-start+1) < batchSize {
			end++
		}
		f.fetchRange(ctx, nums[start], nums[end], byNumber)
		i = end + 1
	}
}

func (f *LogFetcher) fetchRange(ctx context.Context, from, to uint64, byNumber map[uint64]pipeline.BlockRef) {
	q := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(from),
		ToBlock:   new(big.Int).SetUint64(to),
		Addresses: f.Addresses,
	}
	logs, err := f.Client.FilterLogs(ctx, q)
	if err != nil {
		f.logger().Warn("filter logs range", "chain", f.Chain, "from", from, "to", to, "err", err)
		return
	}
	if len(logs) == 0 {
		return
	}
	sort.SliceStable(logs, func(i, j int) bool {
		if logs[i].BlockNumber != logs[j].BlockNumber {
			return logs[i].BlockNumber < logs[j].BlockNumber
		}
		return logs[i].Index < logs[j].Index
	})
	for _, lg := range logs {
		b, ok := byNumber[lg.BlockNumber]
		if !ok {
			continue
		}
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
