package decode

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestRegisterERC20SpecWorks(t *testing.T) {
	d := NewDecoder()
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		Address: common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
		ABI:     "erc20",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.abis["0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"]; !ok {
		t.Fatal("ABI not registered under lowercase key")
	}
}

func TestRegisterInlineJSONWorks(t *testing.T) {
	d := NewDecoder()
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		Address: common.HexToAddress("0x0000000000000000000000000000000000000001"),
		ABI:     `[{"type":"event","name":"Zap","inputs":[]}]`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := d.abis["0x0000000000000000000000000000000000000001"]; !ok {
		t.Fatal("inline ABI not registered")
	}
}

func TestRegisterUnknownSpecFails(t *testing.T) {
	d := NewDecoder()
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		Address: common.HexToAddress("0x0"),
		ABI:     "etherscan",
	})
	if err == nil {
		t.Fatal("expected error for unknown abi kind")
	}
}

func TestRegisterSourcifyWithoutChainIDFails(t *testing.T) {
	d := NewDecoder()
	err := d.Register(context.Background(), MonitorSpec{
		ID:      "m1",
		Address: common.HexToAddress("0x0"),
		ABI:     "sourcify",
	})
	if err == nil {
		t.Fatal("expected error when sourcify used without chain_id")
	}
}
