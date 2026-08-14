package notify

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/config"
)

func BuildRouter(receivers []config.Receiver, logger *slog.Logger) (*Router, error) {
	notifiers := make(map[string]Notifier, len(receivers))
	for _, r := range receivers {
		tmpl := Template{Title: r.Template.Title, Body: r.Template.Body}
		var n Notifier
		switch r.Type {
		case "log":
			n = &LogNotifier{ReceiverID: r.ID, Template: tmpl, Logger: logger}
		case "slack", "telegram", "discord", "pagerduty", "webhook", "generic":
			n = &ShoutrrrNotifier{ReceiverID: r.ID, URL: normalizeShoutrrrURL(r.URL), Template: tmpl}
		default:
			return nil, fmt.Errorf("receiver %s: unsupported type %q (supported: log, slack, telegram, discord, pagerduty, webhook, generic)", r.ID, r.Type)
		}
		if r.Throttle != nil {
			if interval := time.Duration(r.Throttle.MinInterval); interval > 0 {
				n = Throttled(r.ID, n, interval)
			}
		}
		notifiers[r.ID] = n
	}
	return NewRouter(notifiers), nil
}
