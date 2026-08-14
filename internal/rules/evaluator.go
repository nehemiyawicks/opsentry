package rules

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Evaluator interface {
	Eval(ctx context.Context, ev pipeline.Event) ([]pipeline.Match, error)
}
