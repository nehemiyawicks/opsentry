package notify

import (
	"context"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Notifier interface {
	Send(ctx context.Context, receiver string, alert pipeline.Alert) error
}
