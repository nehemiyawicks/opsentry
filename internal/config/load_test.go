package config

import (
	"strings"
	"testing"
)

func TestLoadValid(t *testing.T) {
	cfg, err := Load("testdata/valid.yaml")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Chains) != 1 || cfg.Chains[0].ID != "base" {
		t.Fatalf("unexpected chains: %+v", cfg.Chains)
	}
	if len(cfg.Monitors) != 1 || cfg.Monitors[0].Rules[0].Receivers[0] != "r1" {
		t.Fatalf("unexpected monitors: %+v", cfg.Monitors)
	}
}

func TestValidateRejectsUnknownReceiver(t *testing.T) {
	_, err := Load("testdata/unknown_receiver.yaml")
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist") {
		t.Fatalf("expected error to name the unknown receiver, got: %v", err)
	}
}
