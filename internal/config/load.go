package config

import (
	"bytes"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Chains) == 0 {
		return fmt.Errorf("at least one chain required")
	}
	chains := make(map[string]struct{}, len(c.Chains))
	for _, ch := range c.Chains {
		if ch.ID == "" {
			return fmt.Errorf("chain missing id")
		}
		if _, dup := chains[ch.ID]; dup {
			return fmt.Errorf("duplicate chain id: %s", ch.ID)
		}
		chains[ch.ID] = struct{}{}
	}
	receivers := make(map[string]struct{}, len(c.Receivers))
	for _, r := range c.Receivers {
		if r.ID == "" {
			return fmt.Errorf("receiver missing id")
		}
		receivers[r.ID] = struct{}{}
	}
	for _, m := range c.Monitors {
		if _, ok := chains[m.Chain]; !ok {
			return fmt.Errorf("monitor %s references unknown chain %s", m.ID, m.Chain)
		}
		for i, rule := range m.Rules {
			for _, rcv := range rule.Receivers {
				if _, ok := receivers[rcv]; !ok {
					return fmt.Errorf("monitor %s rule[%d] references unknown receiver %s", m.ID, i, rcv)
				}
			}
		}
	}
	return nil
}
