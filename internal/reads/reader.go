package reads

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type CallClient interface {
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
}

type Def struct {
	Name   string
	Method string
	Output string
}

type Reader struct {
	MonitorID string
	Address   common.Address
	Defs      []Def
	Client    CallClient
	Log       *slog.Logger

	mu          sync.Mutex
	latestBlock uint64
	latest      map[string]any
	prev        map[string]any
}

func (r *Reader) Enrich(ctx context.Context, ev *pipeline.Event) {
	if len(r.Defs) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	blockNum := ev.Log.Block.Number
	if r.latest == nil || blockNum > r.latestBlock {
		newState := make(map[string]any, len(r.Defs))
		for _, def := range r.Defs {
			val, err := r.call(ctx, def, blockNum)
			if err != nil {
				r.logger().Warn("state read failed", "monitor", r.MonitorID, "name", def.Name, "err", err)
				continue
			}
			newState[def.Name] = val
		}
		r.prev = r.latest
		r.latest = newState
		r.latestBlock = blockNum
	}
	ev.State = r.latest
	ev.PrevState = r.prev
}

func (r *Reader) call(ctx context.Context, def Def, block uint64) (any, error) {
	selector := crypto.Keccak256([]byte(def.Method))[:4]
	data, err := r.Client.CallContract(ctx, ethereum.CallMsg{
		To:   &r.Address,
		Data: selector,
	}, new(big.Int).SetUint64(block))
	if err != nil {
		return nil, err
	}
	return decodeReturn(def.Output, data)
}

func (r *Reader) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func decodeReturn(outputType string, data []byte) (any, error) {
	if len(data) < 32 {
		return nil, fmt.Errorf("short return: %d bytes", len(data))
	}
	switch outputType {
	case "bool":
		return data[31] != 0, nil
	case "address":
		return "0x" + hex.EncodeToString(data[12:32]), nil
	case "uint256":
		n := new(big.Int).SetBytes(data[:32])
		f, _ := new(big.Float).SetInt(n).Float64()
		return f, nil
	case "bytes32":
		return "0x" + hex.EncodeToString(data[:32]), nil
	default:
		return nil, fmt.Errorf("unsupported output type %q (supported: bool, address, uint256, bytes32)", outputType)
	}
}
