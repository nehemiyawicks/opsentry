package decode

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

const erc20ABIJSON = `[
  {"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},
  {"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}
]`

type ABIDecoder struct {
	abis     map[string]abi.ABI
	monitors map[string]string
}

func NewERC20Decoder(addrToMonitor map[string]string) (*ABIDecoder, error) {
	a, err := abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse erc20 abi: %w", err)
	}
	abis := make(map[string]abi.ABI, len(addrToMonitor))
	normalized := make(map[string]string, len(addrToMonitor))
	for addr, monitorID := range addrToMonitor {
		k := strings.ToLower(addr)
		abis[k] = a
		normalized[k] = monitorID
	}
	return &ABIDecoder{abis: abis, monitors: normalized}, nil
}

func (d *ABIDecoder) Decode(_ context.Context, log pipeline.Log) (pipeline.Event, error) {
	addrHex := "0x" + strings.ToLower(fmt.Sprintf("%x", log.Address[:]))
	a, ok := d.abis[addrHex]
	if !ok {
		return pipeline.Event{}, fmt.Errorf("no abi registered for %s", addrHex)
	}
	if len(log.Topics) == 0 {
		return pipeline.Event{}, fmt.Errorf("log has no topics")
	}
	ev, err := a.EventByID(common.Hash(log.Topics[0]))
	if err != nil {
		return pipeline.Event{}, fmt.Errorf("unknown event topic %x: %w", log.Topics[0], err)
	}

	var indexed abi.Arguments
	for _, in := range ev.Inputs {
		if in.Indexed {
			indexed = append(indexed, in)
		}
	}
	nonIndexed := ev.Inputs.NonIndexed()

	if len(indexed) != len(log.Topics)-1 {
		return pipeline.Event{}, fmt.Errorf("indexed arg count mismatch: expected %d, got %d", len(indexed), len(log.Topics)-1)
	}

	params := make(map[string]any, len(ev.Inputs))
	if len(indexed) > 0 {
		topics := make([]common.Hash, len(log.Topics)-1)
		for i, t := range log.Topics[1:] {
			topics[i] = common.Hash(t)
		}
		if err := abi.ParseTopicsIntoMap(params, indexed, topics); err != nil {
			return pipeline.Event{}, fmt.Errorf("parse topics: %w", err)
		}
	}
	if len(nonIndexed) > 0 {
		if err := nonIndexed.UnpackIntoMap(params, log.Data); err != nil {
			return pipeline.Event{}, fmt.Errorf("unpack data: %w", err)
		}
	}
	for k, v := range params {
		params[k] = normalize(v)
	}

	return pipeline.Event{
		Log:       log,
		MonitorID: d.monitors[addrHex],
		Name:      ev.Name,
		Params:    params,
	}, nil
}

func normalize(v any) any {
	switch t := v.(type) {
	case *big.Int:
		f, _ := new(big.Float).SetInt(t).Float64()
		return f
	case common.Address:
		return strings.ToLower(t.Hex())
	case common.Hash:
		return strings.ToLower(t.Hex())
	case [32]byte:
		return fmt.Sprintf("0x%x", t[:])
	case [20]byte:
		return fmt.Sprintf("0x%x", t[:])
	}
	return v
}
