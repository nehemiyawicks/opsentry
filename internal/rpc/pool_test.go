package rpc

import (
	"testing"
	"time"
)

func TestPoolSortsByWeightDesc(t *testing.T) {
	a := &Endpoint{URL: "a", Weight: 10}
	b := &Endpoint{URL: "b", Weight: 100}
	c := &Endpoint{URL: "c", Weight: 50}
	p := newPool([]*Endpoint{a, b, c})
	if p.endpoints[0].URL != "b" || p.endpoints[1].URL != "c" || p.endpoints[2].URL != "a" {
		t.Fatalf("expected order [b,c,a] by weight desc, got %v", urls(p.endpoints))
	}
}

func TestPoolMarkFailedSkipsFromAvailable(t *testing.T) {
	a := &Endpoint{URL: "a", Weight: 100}
	b := &Endpoint{URL: "b", Weight: 50}
	now := time.Unix(1000, 0)
	p := newPool([]*Endpoint{a, b})
	p.now = func() time.Time { return now }

	p.markFailed(a)
	avail := p.available()
	if len(avail) != 1 || avail[0].URL != "b" {
		t.Fatalf("expected only b available, got %v", urls(avail))
	}
	now = now.Add(defaultCooldown + time.Second)
	avail = p.available()
	if len(avail) != 2 {
		t.Fatalf("cooldown expired, expected both available, got %v", urls(avail))
	}
}

func TestPoolAllFailedResetsAndReturnsAll(t *testing.T) {
	a := &Endpoint{URL: "a", Weight: 10}
	b := &Endpoint{URL: "b", Weight: 5}
	p := newPool([]*Endpoint{a, b})
	p.now = func() time.Time { return time.Unix(2000, 0) }

	p.markFailed(a)
	p.markFailed(b)
	avail := p.available()
	if len(avail) != 2 {
		t.Fatalf("all-failed should reset and return all, got %v", urls(avail))
	}
	for _, e := range p.endpoints {
		if !e.failedUntil.IsZero() {
			t.Errorf("expected reset failedUntil for %s", e.URL)
		}
	}
}

func urls(es []*Endpoint) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.URL
	}
	return out
}
