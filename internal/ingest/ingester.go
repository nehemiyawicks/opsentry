package ingest

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Ingester interface {
	Run(ctx context.Context, out chan<- pipeline.Log) error
}
