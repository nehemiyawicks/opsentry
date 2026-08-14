package notify

import (
	"testing"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func TestRenderSubstitutesPaths(t *testing.T) {
	a := pipeline.Alert{
		Match: pipeline.Match{
			Event: pipeline.Event{
				MonitorID: "m1",
				Name:      "Transfer",
				Params:    map[string]any{"value": float64(150000e6)},
				Log: pipeline.Log{
					Chain: "base",
					Block: pipeline.BlockRef{Number: 42},
				},
			},
			Severity: "high",
		},
		Kind: pipeline.AlertFiring,
	}
	tmpl := Template{
		Title: "[${severity}] ${monitor.id}",
		Body:  "${event.name} value=${event.params.value} block=${event.log.block.number}",
	}
	title, body := tmpl.Render(a)
	if title != "[high] m1" {
		t.Fatalf("title: %q", title)
	}
	if body != "Transfer value=1.5e+11 block=42" {
		t.Fatalf("body: %q", body)
	}
}

func TestRenderUnknownPathBecomesEmpty(t *testing.T) {
	a := pipeline.Alert{Match: pipeline.Match{Event: pipeline.Event{Name: "X"}}}
	tmpl := Template{Body: "${nope.at.all} tail"}
	_, body := tmpl.Render(a)
	if body != " tail" {
		t.Fatalf("body: %q", body)
	}
}

func TestRenderPassthroughNoTokens(t *testing.T) {
	a := pipeline.Alert{}
	tmpl := Template{Body: "plain text no tokens"}
	_, body := tmpl.Render(a)
	if body != "plain text no tokens" {
		t.Fatalf("body: %q", body)
	}
}
