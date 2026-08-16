package rpc

import (
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"
)

const defaultCooldown = 30 * time.Second

type Endpoint struct {
	URL    string
	Weight int
	eth    *ethclient.Client
	raw    *gethrpc.Client

	mu          sync.Mutex
	failedUntil time.Time
}

type pool struct {
	endpoints []*Endpoint
	cooldown  time.Duration
	now       func() time.Time
}

func newPool(endpoints []*Endpoint) *pool {
	sort.SliceStable(endpoints, func(i, j int) bool {
		return endpoints[i].Weight > endpoints[j].Weight
	})
	return &pool{
		endpoints: endpoints,
		cooldown:  defaultCooldown,
		now:       time.Now,
	}
}

func (p *pool) available() []*Endpoint {
	now := p.now()
	var out []*Endpoint
	for _, e := range p.endpoints {
		e.mu.Lock()
		ok := !now.Before(e.failedUntil)
		e.mu.Unlock()
		if ok {
			out = append(out, e)
		}
	}
	if len(out) == 0 {
		for _, e := range p.endpoints {
			e.mu.Lock()
			e.failedUntil = time.Time{}
			e.mu.Unlock()
		}
		return p.endpoints
	}
	return out
}

func (p *pool) markFailed(e *Endpoint) {
	e.mu.Lock()
	e.failedUntil = p.now().Add(p.cooldown)
	e.mu.Unlock()
}

func (p *pool) close() {
	for _, e := range p.endpoints {
		if e.eth != nil {
			e.eth.Close()
		}
	}
}
