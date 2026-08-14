package config

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRulesetsParse(t *testing.T) {
	matches, err := filepath.Glob("../../rulesets/*/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Skip("no rulesets found; skipping")
	}
	for _, path := range matches {
		path := path
		name := strings.TrimPrefix(path, "../../rulesets/")
		t.Run(name, func(t *testing.T) {
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("load %s: %v", path, err)
			}
			if len(cfg.Monitors) == 0 {
				t.Fatalf("%s has no monitors", path)
			}
			for _, m := range cfg.Monitors {
				if m.Address == "" {
					t.Errorf("%s: monitor %q missing address", path, m.ID)
				}
				if m.ABI == "" {
					t.Errorf("%s: monitor %q missing abi", path, m.ID)
				}
				for i, r := range m.Rules {
					if r.When == "" {
						t.Errorf("%s: monitor %q rule[%d] missing when", path, m.ID, i)
					}
					if r.Severity == "" {
						t.Errorf("%s: monitor %q rule[%d] missing severity", path, m.ID, i)
					}
					if len(r.Receivers) == 0 {
						t.Errorf("%s: monitor %q rule[%d] missing receivers", path, m.ID, i)
					}
				}
			}
		})
	}
}
