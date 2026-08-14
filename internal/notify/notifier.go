package notify

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Notifier interface {
	Send(ctx context.Context, alert pipeline.Alert) error
}
