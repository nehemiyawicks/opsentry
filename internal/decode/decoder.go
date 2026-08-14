package decode

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Decoder interface {
	Decode(ctx context.Context, log pipeline.Log) (pipeline.Event, error)
}
