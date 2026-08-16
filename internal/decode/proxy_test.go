package decode

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
)

const proxyABI = `[
  {"type":"event","name":"Upgraded","inputs":[{"indexed":true,"name":"implementation","type":"address"}]},
  {"type":"event","name":"AdminChanged","inputs":[{"indexed":false,"name":"previousAdmin","type":"address"},{"indexed":false,"name":"newAdmin","type":"address"}]}
]`

const implABI = `[
  {"type":"event","name":"Transfer","inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}]}
]`

type fakeStorage struct {
	slot     []byte
	err      error
	slots    map[common.Hash][]byte
	callResp []byte
	callErr  error
}

func (f *fakeStorage) StorageAt(_ context.Context, _ common.Address, slot common.Hash) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.slots != nil {
		if v, ok := f.slots[slot]; ok {
			return v, nil
		}
		return make([]byte, 32), nil
	}
	return f.slot, nil
}

func (f *fakeStorage) CallContract(_ context.Context, _ ethereum.CallMsg, _ *big.Int) ([]byte, error) {
	if f.callErr != nil {
		return nil, f.callErr
	}
	return f.callResp, nil
}

func TestLooksLikeProxyMatchesEIP1967Events(t *testing.T) {
	d := NewDecoder()
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		Address: common.HexToAddress("0x1"),
		ABI:     proxyABI,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !looksLikeProxy(d.abis["0x0000000000000000000000000000000000000001"]) {
		t.Fatal("expected proxy ABI to be flagged as proxy")
	}
}

func TestReadImplementationHappyPath(t *testing.T) {
	impl := common.HexToAddress("0x2222222222222222222222222222222222222222")
	slot := common.LeftPadBytes(impl.Bytes(), 32)
	fs := &fakeStorage{slot: slot}
	got, ok, err := readImplementationAddress(context.Background(), fs, common.HexToAddress("0x1"))
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got != impl {
		t.Fatalf("expected %s, got %s", impl.Hex(), got.Hex())
	}
}

func TestReadImplementationEmptySlot(t *testing.T) {
	fs := &fakeStorage{slot: make([]byte, 32)}
	_, ok, err := readImplementationAddress(context.Background(), fs, common.HexToAddress("0x1"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("empty slot should return ok=false")
	}
}

func TestReadImplementationBeaconProxy(t *testing.T) {
	beacon := common.HexToAddress("0xbeacon00000000000000000000000000000beac0n")
	impl := common.HexToAddress("0x3333333333333333333333333333333333333333")
	beaconSlotBytes := common.LeftPadBytes(beacon.Bytes(), 32)
	implResp := common.LeftPadBytes(impl.Bytes(), 32)

	fs := &fakeStorage{
		slots: map[common.Hash][]byte{
			eip1967BeaconSlot: beaconSlotBytes,
		},
		callResp: implResp,
	}
	got, ok, err := readImplementationAddress(context.Background(), fs, common.HexToAddress("0x1"))
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if got != impl {
		t.Fatalf("expected impl %s from beacon, got %s", impl.Hex(), got.Hex())
	}
}

func TestReadImplementationBeaconCallFailFallsThrough(t *testing.T) {
	beacon := common.HexToAddress("0xbeacon00000000000000000000000000000beac0n")
	fs := &fakeStorage{
		slots: map[common.Hash][]byte{
			eip1967BeaconSlot: common.LeftPadBytes(beacon.Bytes(), 32),
		},
		callErr: errors.New("call reverted"),
	}
	_, ok, err := readImplementationAddress(context.Background(), fs, common.HexToAddress("0x1"))
	if err == nil {
		t.Fatal("expected error when beacon.implementation() call fails")
	}
	if ok {
		t.Fatal("ok should be false on call error")
	}
}

func TestReadImplementationRPCErrorSurfaced(t *testing.T) {
	fs := &fakeStorage{err: errors.New("rpc down")}
	_, ok, err := readImplementationAddress(context.Background(), fs, common.HexToAddress("0x1"))
	if !ok && err == nil {
		return
	}
	if err == nil {
		t.Fatal("expected error when all slot reads fail and no beacon fallback available")
	}
	if ok {
		t.Fatal("ok should be false on error")
	}
}

func TestSourcifyRegisterFollowsProxyToImplementation(t *testing.T) {
	proxyAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	implAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, strings.ToLower(proxyAddr.Hex()[2:])) ||
			strings.Contains(r.URL.Path, proxyAddr.Hex()[2:]) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{"abi": json.RawMessage(proxyABI)})
			return
		}
		if strings.Contains(r.URL.Path, strings.ToLower(implAddr.Hex()[2:])) ||
			strings.Contains(r.URL.Path, implAddr.Hex()[2:]) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{"abi": json.RawMessage(implABI)})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	d := NewDecoder()
	d.fetcher = &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}

	storage := &fakeStorage{slot: common.LeftPadBytes(implAddr.Bytes(), 32)}
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		ChainID: 8453,
		Address: proxyAddr,
		ABI:     "sourcify",
		Storage: storage,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := d.abis[strings.ToLower(proxyAddr.Hex())]
	if _, ok := got.Events["Transfer"]; !ok {
		t.Fatal("expected Transfer event from implementation ABI (proxy follow failed)")
	}
	if _, ok := got.Events["Upgraded"]; ok {
		t.Fatal("did not expect Upgraded event (should have swapped to implementation)")
	}
}

func TestSourcifyRegisterKeepsProxyABIWhenStorageNil(t *testing.T) {
	proxyAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{"abi": json.RawMessage(proxyABI)})
	}))
	defer srv.Close()

	d := NewDecoder()
	d.fetcher = &SourcifyFetcher{BaseURL: srv.URL, HTTPClient: srv.Client()}

	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		ChainID: 1,
		Address: proxyAddr,
		ABI:     "sourcify",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := d.abis[strings.ToLower(proxyAddr.Hex())]
	if _, ok := got.Events["Upgraded"]; !ok {
		t.Fatal("expected proxy ABI preserved when no StorageReader available")
	}
}
