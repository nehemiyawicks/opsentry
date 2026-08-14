package ingest

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type ReconcileResult struct {
	Canonical []pipeline.BlockRef
	Reverted  []pipeline.BlockRef
}

type Reconciler interface {
	OnHead(ctx context.Context, head pipeline.BlockRef) (ReconcileResult, error)
}
