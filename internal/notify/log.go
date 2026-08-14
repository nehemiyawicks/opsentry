package notify

import (
	"context"
	"log/slog"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type LogNotifier struct {
	ReceiverID string
	Template   Template
	Logger     *slog.Logger
}

func (n *LogNotifier) Send(_ context.Context, alert pipeline.Alert) error {
	title, body := n.Template.Render(alert)
	l := n.Logger
	if l == nil {
		l = slog.Default()
	}
	l.Info("notify",
		"receiver", n.ReceiverID,
		"kind", string(alert.Kind),
		"severity", alert.Match.Severity,
		"monitor", alert.Match.Event.MonitorID,
		"title", title,
		"body", body,
	)
	return nil
}
