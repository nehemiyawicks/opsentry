package rpc

import (
	"context"
	"fmt"
	"math/big"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	gethrpc "github.com/ethereum/go-ethereum/rpc"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Client struct {
	chain string
	eth   *ethclient.Client
	raw   *gethrpc.Client
}

func Dial(ctx context.Context, chain, url string) (*Client, error) {
	c, err := ethclient.DialContext(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("dial %s (%s): %w", chain, url, err)
	}
	return &Client{chain: chain, eth: c, raw: c.Client()}, nil
}

func (c *Client) Close()        { c.eth.Close() }
func (c *Client) Chain() string { return c.chain }

func (c *Client) BlockByNumber(ctx context.Context, n uint64) (pipeline.BlockRef, error) {
	h, err := c.eth.HeaderByNumber(ctx, new(big.Int).SetUint64(n))
	if err != nil {
		return pipeline.BlockRef{}, fmt.Errorf("%s: header %d: %w", c.chain, n, err)
	}
	return toRef(c.chain, h), nil
}

func (c *Client) HeadByTag(ctx context.Context, tag string) (pipeline.BlockRef, error) {
	var h *types.Header
	if err := c.raw.CallContext(ctx, &h, "eth_getBlockByNumber", tag, false); err != nil {
		return pipeline.BlockRef{}, fmt.Errorf("%s: head(%s): %w", c.chain, tag, err)
	}
	if h == nil {
		return pipeline.BlockRef{}, fmt.Errorf("%s: null header for tag %q (node may not expose it)", c.chain, tag)
	}
	return toRef(c.chain, h), nil
}

func (c *Client) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return c.eth.FilterLogs(ctx, q)
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
