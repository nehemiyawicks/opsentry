package decode

import (
	"context"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDecodeUSDCTransfer(t *testing.T) {
	usdc := "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
	d, err := NewERC20Decoder(map[string]string{usdc: "usdc-large-transfer-base"})
	if err != nil {
		t.Fatal(err)
	}

	transferSig := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	var addr [20]byte
	copy(addr[:], common.HexToAddress(usdc).Bytes())
	var topic0 [32]byte
	copy(topic0[:], transferSig.Bytes())
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")
	var topicFrom, topicTo [32]byte
	copy(topicFrom[12:], from.Bytes())
	copy(topicTo[12:], to.Bytes())

	value := big.NewInt(150_000_000_000)
	valueBytes := common.LeftPadBytes(value.Bytes(), 32)

	log := pipeline.Log{
		Chain:    "base",
		Block:    pipeline.BlockRef{Chain: "base", Number: 42},
		Address:  addr,
		Topics:   [][32]byte{topic0, topicFrom, topicTo},
		Data:     valueBytes,
		LogIndex: 5,
	}

	ev, err := d.Decode(context.Background(), log)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Name != "Transfer" {
		t.Fatalf("expected Transfer, got %q", ev.Name)
	}
	if ev.MonitorID != "usdc-large-transfer-base" {
		t.Fatalf("expected monitor id set, got %q", ev.MonitorID)
	}
	if got := ev.Params["from"]; got != strings.ToLower(from.Hex()) {
		t.Fatalf("from mismatch: %v", got)
	}
	if got := ev.Params["to"]; got != strings.ToLower(to.Hex()) {
		t.Fatalf("to mismatch: %v", got)
	}
	fval, ok := ev.Params["value"].(float64)
	if !ok {
		t.Fatalf("expected value as float64, got %T", ev.Params["value"])
	}
	if fval != 150_000_000_000 {
		t.Fatalf("value mismatch: %v", fval)
	}
}

func TestDecodeUnknownAddressFails(t *testing.T) {
	d, _ := NewERC20Decoder(map[string]string{"0x0000000000000000000000000000000000000001": "m"})
	log := pipeline.Log{Address: [20]byte{0x99}, Topics: [][32]byte{{0xaa}}}
	if _, err := d.Decode(context.Background(), log); err == nil {
		t.Fatal("expected error for unregistered address")
	}
}

func TestDecodeUnknownTopicFails(t *testing.T) {
	addr := "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
	d, _ := NewERC20Decoder(map[string]string{addr: "m"})
	var addrB [20]byte
	copy(addrB[:], common.HexToAddress(addr).Bytes())
	log := pipeline.Log{
		Address: addrB,
		Topics:  [][32]byte{{0xde, 0xad, 0xbe, 0xef}},
		Data:    mustHex(t, ""),
	}
	if _, err := d.Decode(context.Background(), log); err == nil {
		t.Fatal("expected error for unknown topic0")
	}
}
