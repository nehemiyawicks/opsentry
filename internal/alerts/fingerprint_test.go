package alerts

import (
	"testing"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func TestFingerprintIncludesBlockHash(t *testing.T) {
	base := pipeline.Match{
		Event: pipeline.Event{
			Log: pipeline.Log{
				Chain:    "base",
				Block:    pipeline.BlockRef{Number: 1, Hash: [32]byte{0x01}},
				TxHash:   [32]byte{0xaa},
				LogIndex: 3,
			},
			MonitorID: "usdc-large-transfer",
		},
		RuleIdx: 0,
	}
	a := Fingerprint(base)

	reorged := base
	reorged.Event.Log.Block.Hash = [32]byte{0x02}
	b := Fingerprint(reorged)

	if a == b {
		t.Fatal("same block number, different block hash must produce different fingerprints")
	}
}

func TestFingerprintDeterministic(t *testing.T) {
	m := pipeline.Match{
		Event: pipeline.Event{
			Log:       pipeline.Log{Chain: "base", Block: pipeline.BlockRef{Hash: [32]byte{0xff}}},
			MonitorID: "m1",
		},
	}
	if Fingerprint(m) != Fingerprint(m) {
		t.Fatal("fingerprint must be deterministic")
	}
}
