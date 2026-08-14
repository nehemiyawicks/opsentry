package notify

import (
	"context"
	"sync"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/obs"
)

type throttled struct {
	receiverID  string
	inner       Notifier
	minInterval time.Duration
	now         func() time.Time

	mu       sync.Mutex
	lastSent time.Time
}

func Throttled(receiverID string, inner Notifier, minInterval time.Duration) Notifier {
	return &throttled{
		receiverID:  receiverID,
		inner:       inner,
		minInterval: minInterval,
		now:         time.Now,
	}
}

func (t *throttled) SendEnv(ctx context.Context, env map[string]any) error {
	t.mu.Lock()
	now := t.now()
	if !t.lastSent.IsZero() && now.Sub(t.lastSent) < t.minInterval {
		t.mu.Unlock()
		obs.AlertsThrottled.WithLabelValues(t.receiverID).Inc()
		return nil
	}
	t.lastSent = now
	t.mu.Unlock()
	return t.inner.SendEnv(ctx, env)
}
