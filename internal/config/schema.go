package config

import (
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Chains    []Chain    `yaml:"chains"`
	Receivers []Receiver `yaml:"receivers"`
	Monitors  []Monitor  `yaml:"monitors"`
}

type Chain struct {
	ID            string        `yaml:"id"`
	ChainID       uint64        `yaml:"chain_id"`
	RPCs          []RPC         `yaml:"rpcs"`
	BlockTimeMs   int           `yaml:"block_time_ms"`
	Confirmations Confirmations `yaml:"confirmations"`
	ReorgCheck    string        `yaml:"reorg_check"`
	Recovery      Recovery      `yaml:"recovery"`
}

type RPC struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

type Confirmations struct {
	Fast      int    `yaml:"fast"`
	Safe      string `yaml:"safe"`
	Finalized string `yaml:"finalized"`
}

type Recovery struct {
	Enabled         bool     `yaml:"enabled"`
	Interval        Duration `yaml:"interval"`
	MaxBlocksPerRun int      `yaml:"max_blocks_per_run"`
	MaxRetries      int      `yaml:"max_retries"`
}

type Receiver struct {
	ID       string           `yaml:"id"`
	Type     string           `yaml:"type"`
	URL      string           `yaml:"url"`
	Template ReceiverTemplate `yaml:"template"`
}

type ReceiverTemplate struct {
	Title string `yaml:"title"`
	Body  string `yaml:"body"`
}

type Monitor struct {
	ID           string `yaml:"id"`
	Chain        string `yaml:"chain"`
	Address      string `yaml:"address"`
	ABI          string `yaml:"abi"`
	Confirmation string `yaml:"confirmation"`
	Reads        []Read `yaml:"reads"`
	Rules        []Rule `yaml:"rules"`
}

type Read struct {
	Name   string `yaml:"name"`
	Method string `yaml:"method"`
}

type Rule struct {
	When      string   `yaml:"when"`
	Severity  string   `yaml:"severity"`
	Receivers []string `yaml:"receivers"`
}

type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

func (d Duration) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}
