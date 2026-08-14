package pipeline

import "fmt"

func EventEnv(ev Event) map[string]any {
	return map[string]any{
		"name":       ev.Name,
		"monitor_id": ev.MonitorID,
		"params":     ev.Params,
		"log": map[string]any{
			"chain":     ev.Log.Chain,
			"address":   fmt.Sprintf("0x%x", ev.Log.Address[:]),
			"tx_hash":   fmt.Sprintf("0x%x", ev.Log.TxHash[:]),
			"tx_index":  ev.Log.TxIndex,
			"log_index": ev.Log.LogIndex,
			"block": map[string]any{
				"number": ev.Log.Block.Number,
				"hash":   fmt.Sprintf("0x%x", ev.Log.Block.Hash[:]),
			},
		},
	}
}

func AlertEnv(a Alert) map[string]any {
	ev := a.Match.Event
	return map[string]any{
		"severity":    a.Match.Severity,
		"kind":        string(a.Kind),
		"fingerprint": a.Fingerprint,
		"monitor": map[string]any{
			"id": ev.MonitorID,
		},
		"event": EventEnv(ev),
	}
}
