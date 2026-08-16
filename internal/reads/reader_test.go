package reads

import (
	"context"
	"encoding/hex"
	"math/big"
	"strings"
	"sync"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type fakeCallClient struct {
	returns map[uint64]map[string][]byte
	mu      sync.Mutex
	calls   int
}

func (f *fakeCallClient) CallContract(_ context.Context, msg ethereum.CallMsg, block *big.Int) ([]byte, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	sel := hex.EncodeToString(msg.Data[:4])
	if r, ok := f.returns[block.Uint64()]; ok {
		if data, ok := r[sel]; ok {
			return data, nil
		}
	}
	return make([]byte, 32), nil
}

func padBool(b bool) []byte {
	out := make([]byte, 32)
	if b {
		out[31] = 1
	}
	return out
}

func padUint(n uint64) []byte {
	out := make([]byte, 32)
	new(big.Int).SetUint64(n).FillBytes(out)
	return out
}

func selectorHex(sig string) string {
	return hex.EncodeToString(crypto.Keccak256([]byte(sig))[:4])
}

func TestReaderEnrichesEventState(t *testing.T) {
	fc := &fakeCallClient{returns: map[uint64]map[string][]byte{
		100: {selectorHex("paused()"): padBool(false)},
	}}
	r := &Reader{
		MonitorID: "m1",
		Address:   common.HexToAddress("0x1"),
		Defs:      []Def{{Name: "paused", Method: "paused()", Output: "bool"}},
		Client:    fc,
	}
	ev := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 100}}}
	r.Enrich(context.Background(), ev)
	if v, _ := ev.State["paused"].(bool); v {
		t.Fatalf("expected paused=false, got %v", ev.State["paused"])
	}
	if fc.calls != 1 {
		t.Fatalf("expected 1 call, got %d", fc.calls)
	}
}

func TestReaderCachesWithinSameBlock(t *testing.T) {
	fc := &fakeCallClient{returns: map[uint64]map[string][]byte{
		100: {selectorHex("paused()"): padBool(true)},
	}}
	r := &Reader{
		Defs:   []Def{{Name: "paused", Method: "paused()", Output: "bool"}},
		Client: fc,
	}
	ctx := context.Background()

	ev1 := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 100}}}
	r.Enrich(ctx, ev1)
	ev2 := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 100}}}
	r.Enrich(ctx, ev2)

	if fc.calls != 1 {
		t.Fatalf("expected 1 call for same-block events, got %d", fc.calls)
	}
}

func TestReaderShiftsPrevOnNewBlock(t *testing.T) {
	fc := &fakeCallClient{returns: map[uint64]map[string][]byte{
		100: {selectorHex("paused()"): padBool(false)},
		101: {selectorHex("paused()"): padBool(true)},
	}}
	r := &Reader{
		Defs:   []Def{{Name: "paused", Method: "paused()", Output: "bool"}},
		Client: fc,
	}
	ctx := context.Background()

	ev1 := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 100}}}
	r.Enrich(ctx, ev1)
	ev2 := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 101}}}
	r.Enrich(ctx, ev2)

	if v, _ := ev2.State["paused"].(bool); !v {
		t.Fatalf("expected latest paused=true, got %v", ev2.State["paused"])
	}
	if v, _ := ev2.PrevState["paused"].(bool); v {
		t.Fatalf("expected prev paused=false, got %v", ev2.PrevState["paused"])
	}
}

func TestReaderDecodesAllSupportedTypes(t *testing.T) {
	addr := common.HexToAddress("0xdeadbeef00000000000000000000000000000042")
	addrBytes := make([]byte, 32)
	copy(addrBytes[12:], addr.Bytes())

	fc := &fakeCallClient{returns: map[uint64]map[string][]byte{
		1: {
			selectorHex("paused()"):      padBool(true),
			selectorHex("owner()"):       addrBytes,
			selectorHex("totalSupply()"): padUint(1_000_000),
			selectorHex("root()"):        {0xaa, 0xbb, 0xcc, 0xdd, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}}
	r := &Reader{
		Defs: []Def{
			{Name: "paused", Method: "paused()", Output: "bool"},
			{Name: "owner", Method: "owner()", Output: "address"},
			{Name: "supply", Method: "totalSupply()", Output: "uint256"},
			{Name: "root", Method: "root()", Output: "bytes32"},
		},
		Client: fc,
	}
	ev := &pipeline.Event{Log: pipeline.Log{Block: pipeline.BlockRef{Number: 1}}}
	r.Enrich(context.Background(), ev)

	if v, _ := ev.State["paused"].(bool); !v {
		t.Errorf("paused: got %v", ev.State["paused"])
	}
	if v, _ := ev.State["owner"].(string); !strings.EqualFold(v, addr.Hex()) {
		t.Errorf("owner: got %v", ev.State["owner"])
	}
	if v, _ := ev.State["supply"].(float64); v != 1_000_000 {
		t.Errorf("supply: got %v", ev.State["supply"])
	}
	if v, _ := ev.State["root"].(string); !strings.HasPrefix(v, "0xaabbccdd") {
		t.Errorf("root: got %v", ev.State["root"])
	}
}

func TestReaderNoDefsIsNoop(t *testing.T) {
	fc := &fakeCallClient{}
	r := &Reader{Client: fc}
	ev := &pipeline.Event{}
	r.Enrich(context.Background(), ev)
	if fc.calls != 0 {
		t.Fatalf("expected zero calls with empty defs, got %d", fc.calls)
	}
	if ev.State != nil {
		t.Fatalf("expected nil state, got %v", ev.State)
	}
}
