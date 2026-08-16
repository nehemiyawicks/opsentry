package decode

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type ABICache interface {
	LoadCachedABI(ctx context.Context, chainID uint64, address common.Address) ([]byte, bool, error)
	SaveCachedABI(ctx context.Context, chainID uint64, address common.Address, abiJSON []byte) error
}

const erc20ABIJSON = `[
  {"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"},
  {"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"spender","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Approval","type":"event"}
]`

type MonitorSpec struct {
	ID      string
	ChainID uint64
	Address common.Address
	ABI     string
	Storage StorageReader
}

type ABIDecoder struct {
	abis      map[string]abi.ABI
	monitors  map[string]string
	fetcher   *SourcifyFetcher
	Etherscan *EtherscanFetcher
	Log       *slog.Logger
	Cache     ABICache
}

func NewDecoder() *ABIDecoder {
	return &ABIDecoder{
		abis:     make(map[string]abi.ABI),
		monitors: make(map[string]string),
		fetcher:  NewSourcifyFetcher(),
	}
}

func (d *ABIDecoder) logger() *slog.Logger {
	if d.Log != nil {
		return d.Log
	}
	return slog.Default()
}

func NewERC20Decoder(addrToMonitor map[string]string) (*ABIDecoder, error) {
	d := NewDecoder()
	for addr, monitorID := range addrToMonitor {
		if err := d.Register(context.Background(), MonitorSpec{
			ID:      monitorID,
			Address: common.HexToAddress(addr),
			ABI:     "erc20",
		}); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *ABIDecoder) Register(ctx context.Context, spec MonitorSpec) error {
	a, err := d.loadABI(ctx, spec)
	if err != nil {
		return fmt.Errorf("monitor %s: %w", spec.ID, err)
	}
	key := strings.ToLower(spec.Address.Hex())
	d.abis[key] = a
	d.monitors[key] = spec.ID
	return nil
}

func (d *ABIDecoder) loadABI(ctx context.Context, spec MonitorSpec) (abi.ABI, error) {
	trimmed := strings.TrimSpace(spec.ABI)
	switch {
	case trimmed == "erc20":
		return abi.JSON(strings.NewReader(erc20ABIJSON))
	case trimmed == "sourcify":
		if spec.ChainID == 0 {
			return abi.ABI{}, fmt.Errorf("sourcify requires chain_id")
		}
		if d.Cache != nil {
			if data, ok, err := d.Cache.LoadCachedABI(ctx, spec.ChainID, spec.Address); err == nil && ok {
				if a, perr := abi.JSON(bytes.NewReader(data)); perr == nil {
					d.logger().Info("abi cache hit", "monitor", spec.ID, "address", spec.Address.Hex())
					return a, nil
				}
			}
		}
		data, err := d.fetcher.FetchJSON(ctx, spec.ChainID, spec.Address)
		if err != nil {
			if d.Etherscan != nil && d.Etherscan.APIKey != "" {
				d.logger().Info("sourcify miss, trying etherscan fallback", "monitor", spec.ID, "err", err)
				if esData, esErr := d.Etherscan.FetchJSON(ctx, spec.ChainID, spec.Address); esErr == nil {
					data = esData
				} else {
					return abi.ABI{}, fmt.Errorf("both sourcify and etherscan failed: sourcify=%v; etherscan=%v", err, esErr)
				}
			} else {
				return abi.ABI{}, err
			}
		}
		a, err := abi.JSON(bytes.NewReader(data))
		if err != nil {
			return abi.ABI{}, fmt.Errorf("parse abi: %w", err)
		}
		finalData := data
		finalABI := a
		if looksLikeProxy(a) {
			log := d.logger().With("monitor", spec.ID, "proxy", spec.Address.Hex())
			switch {
			case spec.Storage == nil:
				log.Warn("sourcify returned a proxy ABI but no StorageReader available; using proxy ABI")
			default:
				impl, ok, rerr := readImplementationAddress(ctx, spec.Storage, spec.Address)
				switch {
				case rerr != nil:
					log.Warn("proxy implementation slot read failed; using proxy ABI", "err", rerr)
				case !ok:
					log.Warn("proxy detected but no known impl slot populated (EIP-1967 or OZ unstructured); using proxy ABI. If this is a token, use abi: erc20")
				default:
					implData, ferr := d.fetcher.FetchJSON(ctx, spec.ChainID, impl)
					if ferr != nil {
						log.Warn("proxy resolved but implementation ABI not on Sourcify; using proxy ABI", "impl", impl.Hex(), "err", ferr)
					} else if implABI, perr := abi.JSON(bytes.NewReader(implData)); perr != nil {
						log.Warn("proxy impl ABI parse failed; using proxy ABI", "impl", impl.Hex(), "err", perr)
					} else {
						log.Info("resolved proxy to implementation via Sourcify", "impl", impl.Hex())
						finalData = implData
						finalABI = implABI
					}
				}
			}
		}
		if d.Cache != nil {
			if err := d.Cache.SaveCachedABI(ctx, spec.ChainID, spec.Address, finalData); err != nil {
				d.logger().Warn("abi cache save failed", "monitor", spec.ID, "err", err)
			}
		}
		return finalABI, nil
	case strings.HasPrefix(trimmed, "["):
		return abi.JSON(strings.NewReader(trimmed))
	default:
		return abi.ABI{}, fmt.Errorf("unsupported abi %q (use 'erc20', 'sourcify', or inline JSON)", spec.ABI)
	}
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
