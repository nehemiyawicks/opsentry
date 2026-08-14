package alerts

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Manager interface {
	Handle(ctx context.Context, m pipeline.Match) (pipeline.Alert, bool, error)
	HandleReverted(ctx context.Context, m pipeline.Match) (pipeline.Alert, bool, error)
}
