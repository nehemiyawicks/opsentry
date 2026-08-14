package notify

import "context"

type Notifier interface {
	SendEnv(ctx context.Context, env map[string]any) error
}
