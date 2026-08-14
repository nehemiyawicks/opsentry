package alerts

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

func Fingerprint(m pipeline.Match) string {
	h := sha256.New()
	h.Write([]byte(m.Event.Log.Chain))
	h.Write([]byte{0})
	h.Write([]byte(m.Event.MonitorID))
	h.Write([]byte{0})
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(m.RuleIdx))
	h.Write(buf[:])
	h.Write(m.Event.Log.Block.Hash[:])
	h.Write(m.Event.Log.TxHash[:])
	binary.BigEndian.PutUint64(buf[:], uint64(m.Event.Log.LogIndex))
	h.Write(buf[:])
	return hex.EncodeToString(h.Sum(nil))
}
