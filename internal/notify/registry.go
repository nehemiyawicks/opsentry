package notify

import (
	"fmt"
	"log/slog"

	"github.com/nehemiyawicks/opsentry/internal/config"
)

func BuildRouter(receivers []config.Receiver, logger *slog.Logger) (*Router, error) {
	notifiers := make(map[string]Notifier, len(receivers))
	for _, r := range receivers {
		tmpl := Template{Title: r.Template.Title, Body: r.Template.Body}
		switch r.Type {
		case "log":
			notifiers[r.ID] = &LogNotifier{ReceiverID: r.ID, Template: tmpl, Logger: logger}
		case "slack", "telegram", "discord", "pagerduty", "webhook", "generic":
			notifiers[r.ID] = &ShoutrrrNotifier{ReceiverID: r.ID, URL: normalizeShoutrrrURL(r.URL), Template: tmpl}
		default:
			return nil, fmt.Errorf("receiver %s: unsupported type %q (supported: log, slack, telegram, discord, pagerduty, webhook, generic)", r.ID, r.Type)
		}
	}
	return NewRouter(notifiers), nil
}
