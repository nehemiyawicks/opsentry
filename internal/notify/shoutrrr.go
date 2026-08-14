package notify

import (
	"context"
	"fmt"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type ShoutrrrNotifier struct {
	URLs map[string]string
}

func (n *ShoutrrrNotifier) Send(ctx context.Context, receiver string, alert pipeline.Alert) error {
	if _, ok := n.URLs[receiver]; !ok {
		return fmt.Errorf("unknown receiver: %s", receiver)
	}
	_ = ctx
	_ = alert
	return nil
}
