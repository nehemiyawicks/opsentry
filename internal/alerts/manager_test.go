package alerts

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

func mkMatch() pipeline.Match {
	return pipeline.Match{
		Event: pipeline.Event{
			MonitorID: "m1",
			Name:      "Transfer",
			Log: pipeline.Log{
				Chain:    "base",
				Block:    pipeline.BlockRef{Chain: "base", Number: 100, Hash: [32]byte{0xaa}},
				TxHash:   [32]byte{0xbb},
				LogIndex: 3,
			},
		},
		Severity:  "high",
		Receivers: []string{"r1"},
	}
}

func newStore(t *testing.T) storage.Store {
	t.Helper()
	s, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestFirstMatchFiresSecondDedups(t *testing.T) {
	m := &Manager{Store: newStore(t), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }}
	ctx := context.Background()
	match := mkMatch()

	alert, fired, err := m.Handle(ctx, match)
	if err != nil {
		t.Fatal(err)
	}
	if !fired {
		t.Fatal("first match should fire")
	}
	if alert.Kind != pipeline.AlertFiring {
		t.Fatalf("expected Firing, got %v", alert.Kind)
	}

	_, fired2, err := m.Handle(ctx, match)
	if err != nil {
		t.Fatal(err)
	}
	if fired2 {
		t.Fatal("second identical match should be deduped")
	}
}

func TestDifferentBlockHashProducesDifferentFingerprint(t *testing.T) {
	m := &Manager{Store: newStore(t)}
	ctx := context.Background()

	match := mkMatch()
	_, fired, _ := m.Handle(ctx, match)
	if !fired {
		t.Fatal("first should fire")
	}

	reorged := match
	reorged.Event.Log.Block.Hash = [32]byte{0xff}
	_, fired2, _ := m.Handle(ctx, reorged)
	if !fired2 {
		t.Fatal("reorged replay should fire as a new alert (different blockHash)")
	}
}

func TestHandleRevertedRecords(t *testing.T) {
	m := &Manager{Store: newStore(t)}
	ctx := context.Background()
	match := mkMatch()

	alert, _, err := m.HandleReverted(ctx, match)
	if err != nil {
		t.Fatal(err)
	}
	if alert.Kind != pipeline.AlertReverted {
		t.Fatalf("expected Reverted, got %v", alert.Kind)
	}
}
