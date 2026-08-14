package notify

import (
	"context"
	"fmt"
	"log/slog"
)

type LogNotifier struct {
	ReceiverID string
	Template   Template
	Logger     *slog.Logger
}

func (n *LogNotifier) SendEnv(_ context.Context, env map[string]any) error {
	title, body := n.Template.Render(env)
	l := n.Logger
	if l == nil {
		l = slog.Default()
	}
	l.Info("notify",
		"receiver", n.ReceiverID,
		"kind", asString(env["kind"]),
		"severity", asString(env["severity"]),
		"monitor", monitorID(env),
		"title", title,
		"body", body,
	)
	return nil
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

func monitorID(env map[string]any) string {
	m, ok := env["monitor"].(map[string]any)
	if !ok {
		return ""
	}
	return asString(m["id"])
}
