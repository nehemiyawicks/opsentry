package pipeline

import "time"

type BlockRef struct {
	Chain      string
	Number     uint64
	Hash       [32]byte
	ParentHash [32]byte
	Time       time.Time
}

type Log struct {
	Chain    string
	Block    BlockRef
	TxHash   [32]byte
	TxIndex  uint
	LogIndex uint
	Address  [20]byte
	Topics   [][32]byte
	Data     []byte
}

type Event struct {
	Log          Log
	MonitorID    string
	Name         string
	Params       map[string]any
	State        map[string]any
	PrevState    map[string]any
	Confirmation string
}

type Match struct {
	Event     Event
	RuleIdx   int
	Severity  string
	Receivers []string
}

type AlertKind string

const (
	AlertFiring   AlertKind = "firing"
	AlertResolved AlertKind = "resolved"
	AlertReverted AlertKind = "reverted"
)

type Alert struct {
	Match       Match
	Fingerprint string
	Kind        AlertKind
	At          time.Time
}
