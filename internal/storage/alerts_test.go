package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func mkAlert(fingerprint string, block uint64, hash byte, receivers []string) pipeline.Alert {
	return pipeline.Alert{
		Fingerprint: fingerprint,
		Kind:        pipeline.AlertFiring,
		At:          time.Unix(1_700_000_000, 0).UTC(),
		Match: pipeline.Match{
			Event: pipeline.Event{
				MonitorID: "m1",
				Name:      "Transfer",
				Params:    map[string]any{"value": float64(1_000_000)},
				Log: pipeline.Log{
					Chain: "base",
					Block: pipeline.BlockRef{Chain: "base", Number: block, Hash: [32]byte{hash}},
				},
			},
			Severity:  "high",
			Receivers: receivers,
		},
	}
}

func TestLoadAlertsAtBlockReturnsMatching(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	if err := s.RecordAlert(ctx, mkAlert("fp1", 100, 0xaa, []string{"r1", "r2"})); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAlert(ctx, mkAlert("fp2", 100, 0xaa, []string{"r1"})); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordAlert(ctx, mkAlert("fp3", 101, 0xbb, []string{"r1"})); err != nil {
		t.Fatal(err)
	}

	got, err := s.LoadAlertsAtBlock(ctx, "base", 100, [32]byte{0xaa})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 alerts at block 100 with hash aa, got %d: %+v", len(got), got)
	}
	seen := map[string][]string{}
	for _, sa := range got {
		seen[sa.Fingerprint] = sa.Receivers
	}
	if len(seen["fp1"]) != 2 || seen["fp1"][0] != "r1" {
		t.Fatalf("fp1 receivers unexpected: %+v", seen["fp1"])
	}
	if len(seen["fp2"]) != 1 || seen["fp2"][0] != "r1" {
		t.Fatalf("fp2 receivers unexpected: %+v", seen["fp2"])
	}
	if v := got[0].Env; v == nil {
		t.Fatal("expected env populated from payload")
	}
}

func TestLoadAlertsAtBlockIgnoresDifferentHash(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	if err := s.RecordAlert(ctx, mkAlert("fp1", 100, 0xaa, []string{"r1"})); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadAlertsAtBlock(ctx, "base", 100, [32]byte{0xff})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 alerts (wrong hash), got %d", len(got))
	}
}
