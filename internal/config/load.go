package config

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

var addressRegex = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

var validConfirmations = map[string]bool{
	"":          true,
	"fast":      true,
	"safe":      true,
	"finalized": true,
}

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
		if ch.ChainID == 0 {
			return fmt.Errorf("chain %s: chain_id must be > 0", ch.ID)
		}
		for i, r := range ch.RPCs {
			if r.URL == "" {
				return fmt.Errorf("chain %s: rpcs[%d]: url is required", ch.ID, i)
			}
			u, err := url.Parse(r.URL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "ws" && u.Scheme != "wss") {
				return fmt.Errorf("chain %s: rpcs[%d]: url %q must have http(s):// or ws(s):// scheme", ch.ID, i, r.URL)
			}
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
	monitors := make(map[string]struct{}, len(c.Monitors))
	for _, m := range c.Monitors {
		if m.ID == "" {
			return fmt.Errorf("monitor missing id")
		}
		if _, dup := monitors[m.ID]; dup {
			return fmt.Errorf("duplicate monitor id: %s", m.ID)
		}
		monitors[m.ID] = struct{}{}
		if _, ok := chains[m.Chain]; !ok {
			return fmt.Errorf("monitor %s references unknown chain %s", m.ID, m.Chain)
		}
		if m.Address != "" && !addressRegex.MatchString(strings.TrimSpace(m.Address)) {
			return fmt.Errorf("monitor %s: address %q must be 0x followed by 40 hex chars", m.ID, m.Address)
		}
		if !validConfirmations[m.Confirmation] {
			return fmt.Errorf("monitor %s: confirmation %q must be one of \"fast\", \"safe\", \"finalized\" (or empty for default)", m.ID, m.Confirmation)
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
