package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Client struct {
	chain string
	pool  *pool
}

type EndpointSpec struct {
	URL    string
	Weight int
}

func Dial(ctx context.Context, chain string, endpoints []EndpointSpec) (*Client, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("dial %s: no endpoints provided", chain)
	}
	var pooled []*Endpoint
	var lastErr error
	for _, ep := range endpoints {
		eth, err := ethclient.DialContext(ctx, ep.URL)
		if err != nil {
			lastErr = err
			continue
		}
		pooled = append(pooled, &Endpoint{URL: ep.URL, Weight: ep.Weight, eth: eth, raw: eth.Client()})
	}
	if len(pooled) == 0 {
		return nil, fmt.Errorf("dial %s: no endpoints reachable (last: %w)", chain, lastErr)
	}
	return &Client{chain: chain, pool: newPool(pooled)}, nil
}

func DialSingle(ctx context.Context, chain, url string) (*Client, error) {
	return Dial(ctx, chain, []EndpointSpec{{URL: url, Weight: 100}})
}

func (c *Client) Close()        { c.pool.close() }
func (c *Client) Chain() string { return c.chain }

func (c *Client) BlockByNumber(ctx context.Context, n uint64) (pipeline.BlockRef, error) {
	return tryFailover(c, func(e *Endpoint) (pipeline.BlockRef, error) {
		h, err := e.eth.HeaderByNumber(ctx, new(big.Int).SetUint64(n))
		if err != nil {
			return pipeline.BlockRef{}, err
		}
		return toRef(c.chain, h), nil
	}, fmt.Sprintf("header %d", n))
}

func (c *Client) HeadByTag(ctx context.Context, tag string) (pipeline.BlockRef, error) {
	ref, err := tryFailover(c, func(e *Endpoint) (pipeline.BlockRef, error) {
		var h *types.Header
		if err := e.raw.CallContext(ctx, &h, "eth_getBlockByNumber", tag, false); err != nil {
			return pipeline.BlockRef{}, err
		}
		if h == nil {
			return pipeline.BlockRef{}, fmt.Errorf("null header for tag %q", tag)
		}
		return toRef(c.chain, h), nil
	}, fmt.Sprintf("head(%s)", tag))
	return ref, err
}

func (c *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return tryFailover(c, func(e *Endpoint) ([]types.Log, error) {
		return e.eth.FilterLogs(ctx, q)
	}, "filter logs")
}

func (c *Client) StorageAt(ctx context.Context, address common.Address, slot common.Hash) ([]byte, error) {
	return tryFailover(c, func(e *Endpoint) ([]byte, error) {
		return e.eth.StorageAt(ctx, address, slot, nil)
	}, "storage at")
}

func (c *Client) CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	return tryFailover(c, func(e *Endpoint) ([]byte, error) {
		return e.eth.CallContract(ctx, msg, blockNumber)
	}, "call contract")
}

func tryFailover[T any](c *Client, fn func(*Endpoint) (T, error), what string) (T, error) {
	var zero T
	var lastErr error
	for _, e := range c.pool.available() {
		v, err := fn(e)
		if err == nil {
			return v, nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return zero, err
		}
		c.pool.markFailed(e)
		lastErr = err
	}
	if lastErr == nil {
		return zero, fmt.Errorf("%s: no endpoints available", c.chain)
	}
	return zero, fmt.Errorf("%s: %s: all endpoints failed (last: %w)", c.chain, what, lastErr)
}

func toRef(chain string, h *types.Header) pipeline.BlockRef {
	var hash, parent [32]byte
	copy(hash[:], h.Hash().Bytes())
	copy(parent[:], h.ParentHash.Bytes())
	return pipeline.BlockRef{
		Chain:      chain,
		Number:     h.Number.Uint64(),
		Hash:       hash,
		ParentHash: parent,
		Time:       time.Unix(int64(h.Time), 0).UTC(),
	}
}
