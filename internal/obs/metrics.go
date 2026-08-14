package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HeadLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "opsentry_head_lag_blocks", Help: "Blocks behind chain head, per chain and confirmation policy"},
		[]string{"chain", "confirmation"},
	)
	AlertsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "opsentry_alerts_sent_total", Help: "Alerts delivered, per receiver and kind"},
		[]string{"receiver", "kind"},
	)
	AlertsDeduped = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "opsentry_alerts_deduped_total", Help: "Alerts suppressed by dedup, per monitor"},
		[]string{"monitor"},
	)
	AlertsThrottled = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "opsentry_alerts_throttled_total", Help: "Alerts suppressed by receiver throttle, per receiver"},
		[]string{"receiver"},
	)
	ReorgsSeen = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "opsentry_reorgs_seen_total", Help: "Reorgs detected, per chain and depth bucket"},
		[]string{"chain", "depth"},
	)
	RPCErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "opsentry_rpc_errors_total", Help: "RPC errors, per chain and rpc url"},
		[]string{"chain", "rpc"},
	)
)

func init() {
	prometheus.MustRegister(HeadLag, AlertsSent, AlertsDeduped, AlertsThrottled, ReorgsSeen, RPCErrors)
}

func Handler() http.Handler {
	return promhttp.Handler()
}
