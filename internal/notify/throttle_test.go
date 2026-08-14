package notify

import (
	"context"
	"sync"
	"testing"
	"time"
)

type countingNotifier struct {
	sent int
	mu   sync.Mutex
}

func (c *countingNotifier) SendEnv(_ context.Context, _ map[string]any) error {
	c.mu.Lock()
	c.sent++
	c.mu.Unlock()
	return nil
}

func TestFirstSendPassesThrough(t *testing.T) {
	inner := &countingNotifier{}
	tn := Throttled("r1", inner, 5*time.Second).(*throttled)
	tn.now = func() time.Time { return time.Unix(1000, 0) }

	_ = tn.SendEnv(context.Background(), nil)
	if inner.sent != 1 {
		t.Fatalf("expected 1 send, got %d", inner.sent)
	}
}

func TestSecondSendWithinIntervalDropped(t *testing.T) {
	inner := &countingNotifier{}
	tn := Throttled("r1", inner, 5*time.Second).(*throttled)
	now := time.Unix(1000, 0)
	tn.now = func() time.Time { return now }

	_ = tn.SendEnv(context.Background(), nil)
	now = now.Add(2 * time.Second)
	_ = tn.SendEnv(context.Background(), nil)
	if inner.sent != 1 {
		t.Fatalf("expected 1 send (second dropped), got %d", inner.sent)
	}
}

func TestSendAfterIntervalPasses(t *testing.T) {
	inner := &countingNotifier{}
	tn := Throttled("r1", inner, 5*time.Second).(*throttled)
	now := time.Unix(1000, 0)
	tn.now = func() time.Time { return now }

	_ = tn.SendEnv(context.Background(), nil)
	now = now.Add(6 * time.Second)
	_ = tn.SendEnv(context.Background(), nil)
	if inner.sent != 2 {
		t.Fatalf("expected 2 sends (both allowed), got %d", inner.sent)
	}
}

func TestConcurrentSendsAreSafe(t *testing.T) {
	inner := &countingNotifier{}
	tn := Throttled("r1", inner, time.Millisecond).(*throttled)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = tn.SendEnv(context.Background(), nil)
		}()
	}
	wg.Wait()
	if inner.sent < 1 {
		t.Fatalf("expected at least one send, got %d", inner.sent)
	}
	if inner.sent > 100 {
		t.Fatalf("impossible: got %d sends from 100 attempts", inner.sent)
	}
}
