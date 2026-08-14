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
	IsDuplicate(ctx context.Context, fingerprint string) (bool, error)
	RecordAlert(ctx context.Context, alert pipeline.Alert) error
	Close() error
}
