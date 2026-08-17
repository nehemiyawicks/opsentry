package pipeline

import "testing"

func TestEventEnvExposesExpectedKeys(t *testing.T) {
	ev := Event{
		MonitorID: "m1",
		Name:      "Transfer",
		Params:    map[string]any{"value": 42.0},
		State:     map[string]any{"paused": false},
		PrevState: map[string]any{"paused": true},
		Log: Log{
			Chain:    "base",
			Address:  [20]byte{0xaa},
			TxHash:   [32]byte{0xbb},
			LogIndex: 5,
			Block:    BlockRef{Chain: "base", Number: 100, Hash: [32]byte{0xcc}},
		},
	}
	env := EventEnv(ev)

	if env["name"] != "Transfer" {
		t.Errorf("name: %v", env["name"])
	}
	if env["monitor_id"] != "m1" {
		t.Errorf("monitor_id: %v", env["monitor_id"])
	}
	params, _ := env["params"].(map[string]any)
	if params["value"] != 42.0 {
		t.Errorf("params.value: %v", params["value"])
	}
	state, _ := env["state"].(map[string]any)
	if state["paused"] != false {
		t.Errorf("state.paused: %v", state["paused"])
	}
	prev, _ := env["prev"].(map[string]any)
	prevState, _ := prev["state"].(map[string]any)
	if prevState["paused"] != true {
		t.Errorf("prev.state.paused: %v", prevState["paused"])
	}
	log, _ := env["log"].(map[string]any)
	if log["chain"] != "base" {
		t.Errorf("log.chain: %v", log["chain"])
	}
	block, _ := log["block"].(map[string]any)
	if block["number"] != uint64(100) {
		t.Errorf("log.block.number: %v", block["number"])
	}
}

func TestEventEnvNilStateBecomesEmptyMap(t *testing.T) {
	ev := Event{Name: "X"}
	env := EventEnv(ev)
	state, ok := env["state"].(map[string]any)
	if !ok {
		t.Fatal("state should default to empty map, not nil")
	}
	if len(state) != 0 {
		t.Errorf("state should be empty, got %v", state)
	}
	prev, _ := env["prev"].(map[string]any)
	prevState, ok := prev["state"].(map[string]any)
	if !ok {
		t.Fatal("prev.state should default to empty map, not nil")
	}
	if len(prevState) != 0 {
		t.Errorf("prev.state should be empty, got %v", prevState)
	}
}

func TestAlertEnvIncludesAlertMetadata(t *testing.T) {
	a := Alert{
		Match: Match{
			Event:    Event{MonitorID: "m1", Name: "T"},
			Severity: "critical",
		},
		Fingerprint: "abc123",
		Kind:        AlertFiring,
	}
	env := AlertEnv(a)

	if env["severity"] != "critical" {
		t.Errorf("severity: %v", env["severity"])
	}
	if env["kind"] != "firing" {
		t.Errorf("kind: %v", env["kind"])
	}
	if env["fingerprint"] != "abc123" {
		t.Errorf("fingerprint: %v", env["fingerprint"])
	}
	monitor, _ := env["monitor"].(map[string]any)
	if monitor["id"] != "m1" {
		t.Errorf("monitor.id: %v", monitor["id"])
	}
	if _, ok := env["event"].(map[string]any); !ok {
		t.Errorf("event should be a map, got %T", env["event"])
	}
}
