package rules

import (
	"context"
	"testing"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func makeEvent(name string, params map[string]any) pipeline.Event {
	return pipeline.Event{
		MonitorID: "m1",
		Name:      name,
		Params:    params,
		Log:       pipeline.Log{Chain: "base"},
	}
}

func TestRuleMatchesOnEventName(t *testing.T) {
	e, err := NewExprEvaluator([]MonitorRules{{
		MonitorID: "m1",
		Rules: []Rule{{
			When:      `event.name == "Transfer"`,
			Severity:  "low",
			Receivers: []string{"r1"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := e.Eval(context.Background(), makeEvent("Transfer", nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0].Severity != "low" {
		t.Fatalf("expected 1 match, got %+v", matches)
	}
	other, _ := e.Eval(context.Background(), makeEvent("Approval", nil))
	if len(other) != 0 {
		t.Fatalf("expected no match for Approval, got %+v", other)
	}
}

func TestRuleFiltersByThreshold(t *testing.T) {
	e, err := NewExprEvaluator([]MonitorRules{{
		MonitorID: "m1",
		Rules: []Rule{{
			When:      `event.name == "Transfer" && event.params.value > 100000e6`,
			Severity:  "high",
			Receivers: []string{"slack"},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	below, _ := e.Eval(context.Background(), makeEvent("Transfer", map[string]any{"value": 99999e6}))
	if len(below) != 0 {
		t.Fatal("below-threshold should not match")
	}
	above, _ := e.Eval(context.Background(), makeEvent("Transfer", map[string]any{"value": 500000e6}))
	if len(above) != 1 || above[0].Severity != "high" {
		t.Fatalf("above-threshold should match: %+v", above)
	}
}

func TestRuleScopedToMonitor(t *testing.T) {
	e, err := NewExprEvaluator([]MonitorRules{
		{MonitorID: "m1", Rules: []Rule{{When: `true`, Severity: "a", Receivers: []string{"r"}}}},
		{MonitorID: "m2", Rules: []Rule{{When: `true`, Severity: "b", Receivers: []string{"r"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := pipeline.Event{MonitorID: "m1", Name: "X"}
	matches, _ := e.Eval(context.Background(), ev)
	if len(matches) != 1 || matches[0].Severity != "a" {
		t.Fatalf("only m1 should match m1's event: %+v", matches)
	}
}

func TestCompileErrorFailsFast(t *testing.T) {
	_, err := NewExprEvaluator([]MonitorRules{{
		MonitorID: "m1",
		Rules:     []Rule{{When: `event.name ==`, Severity: "x", Receivers: []string{"r"}}},
	}})
	if err == nil {
		t.Fatal("expected compile error for malformed expression")
	}
}

func TestRuleRuntimeErrorSuppressed(t *testing.T) {
	e, err := NewExprEvaluator([]MonitorRules{{
		MonitorID: "m1",
		Rules:     []Rule{{When: `event.params.value + 1`, Severity: "x", Receivers: []string{"r"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := e.Eval(context.Background(), makeEvent("Transfer", map[string]any{"value": "not a number"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatal("runtime error should suppress match, not crash")
	}
}
