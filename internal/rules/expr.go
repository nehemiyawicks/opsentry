package rules

import (
	"context"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type Rule struct {
	When      string
	Severity  string
	Receivers []string
}

type MonitorRules struct {
	MonitorID string
	Rules     []Rule
}

type compiled struct {
	monitorID string
	idx       int
	severity  string
	receivers []string
	program   *vm.Program
}

type ExprEvaluator struct {
	rules []compiled
}

func NewExprEvaluator(monitors []MonitorRules) (*ExprEvaluator, error) {
	var rules []compiled
	for _, m := range monitors {
		for i, r := range m.Rules {
			prog, err := expr.Compile(r.When,
				expr.AsBool(),
				expr.AllowUndefinedVariables(),
			)
			if err != nil {
				return nil, fmt.Errorf("compile %s[%d] (%q): %w", m.MonitorID, i, r.When, err)
			}
			rules = append(rules, compiled{
				monitorID: m.MonitorID,
				idx:       i,
				severity:  r.Severity,
				receivers: r.Receivers,
				program:   prog,
			})
		}
	}
	return &ExprEvaluator{rules: rules}, nil
}

func (e *ExprEvaluator) Eval(_ context.Context, ev pipeline.Event) ([]pipeline.Match, error) {
	env := BuildEnv(ev)
	var out []pipeline.Match
	for _, r := range e.rules {
		if r.monitorID != ev.MonitorID {
			continue
		}
		v, err := expr.Run(r.program, env)
		if err != nil {
			continue
		}
		matched, _ := v.(bool)
		if !matched {
			continue
		}
		out = append(out, pipeline.Match{
			Event:     ev,
			RuleIdx:   r.idx,
			Severity:  r.severity,
			Receivers: r.receivers,
		})
	}
	return out, nil
}

func BuildEnv(ev pipeline.Event) map[string]any {
	return map[string]any{"event": pipeline.EventEnv(ev)}
}
