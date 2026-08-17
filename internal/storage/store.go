package storage

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Cursor struct {
	Chain string
	Block pipeline.BlockRef
}

type Store interface {
	LoadCursor(ctx context.Context, chain string) (Cursor, bool, error)
	SaveCursor(ctx context.Context, c Cursor) error
	RememberBlock(ctx context.Context, b pipeline.BlockRef) error
	LoadRecentBlocks(ctx context.Context, chain string, minBlockNumber uint64) ([]pipeline.BlockRef, error)
	LoadCanonicalBlocksRange(ctx context.Context, chain string, fromInclusive, toInclusive uint64) ([]pipeline.BlockRef, error)
	IsDuplicate(ctx context.Context, fingerprint string) (bool, error)
	RecordAlert(ctx context.Context, alert pipeline.Alert) error
	LoadAlertsAtBlock(ctx context.Context, chain string, blockNumber uint64, blockHash [32]byte) ([]StoredAlert, error)
	LoadCachedABI(ctx context.Context, chainID uint64, address [20]byte) ([]byte, bool, error)
	SaveCachedABI(ctx context.Context, chainID uint64, address [20]byte, abiJSON []byte) error
	ClearABICache(ctx context.Context) (int, error)
	Close() error
}

type StoredAlert struct {
	Fingerprint string
	MonitorID   string
	Severity    string
	Receivers   []string
	Env         map[string]any
}
