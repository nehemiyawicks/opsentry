package notify

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/containrrr/shoutrrr"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type ShoutrrrNotifier struct {
	ReceiverID string
	URL        string
	Template   Template
}

func (n *ShoutrrrNotifier) Send(_ context.Context, alert pipeline.Alert) error {
	title, body := n.Template.Render(alert)
	msg := body
	if title != "" {
		msg = title + "\n" + body
	}
	errs := shoutrrr.Send(n.URL, msg)
	if errs != nil {
		return fmt.Errorf("shoutrrr %s: %w", n.ReceiverID, errs)
	}
	return nil
}

var errUnknownReceiver = errors.New("unknown receiver")

type Router struct {
	notifiers map[string]Notifier
}

func NewRouter(notifiers map[string]Notifier) *Router {
	return &Router{notifiers: notifiers}
}

func (r *Router) Send(ctx context.Context, receiverID string, alert pipeline.Alert) error {
	n, ok := r.notifiers[receiverID]
	if !ok {
		return fmt.Errorf("%w: %s", errUnknownReceiver, receiverID)
	}
	return n.Send(ctx, alert)
}

func (r *Router) IDs() []string {
	out := make([]string, 0, len(r.notifiers))
	for id := range r.notifiers {
		out = append(out, id)
	}
	return out
}

func IsUnknownReceiver(err error) bool { return errors.Is(err, errUnknownReceiver) }

func normalizeShoutrrrURL(u string) string {
	return strings.TrimSpace(u)
}
