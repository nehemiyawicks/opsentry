package alerts

import (
	"context"
	"time"

	"github.com/nehemiyawicks/opsentry/internal/obs"
	"github.com/nehemiyawicks/opsentry/internal/pipeline"
	"github.com/nehemiyawicks/opsentry/internal/storage"
)

type Manager struct {
	Store storage.Store
	Now   func() time.Time
}

func (m *Manager) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now().UTC()
}

func (m *Manager) Handle(ctx context.Context, match pipeline.Match) (pipeline.Alert, bool, error) {
	fp := Fingerprint(match)
	dup, err := m.Store.IsDuplicate(ctx, fp)
	if err != nil {
		return pipeline.Alert{}, false, err
	}
	if dup {
		obs.AlertsDeduped.WithLabelValues(match.Event.MonitorID).Inc()
		return pipeline.Alert{}, false, nil
	}
	alert := pipeline.Alert{
		Match:       match,
		Fingerprint: fp,
		Kind:        pipeline.AlertFiring,
		At:          m.now(),
	}
	if err := m.Store.RecordAlert(ctx, alert); err != nil {
		return pipeline.Alert{}, false, err
	}
	return alert, true, nil
}

func (m *Manager) HandleReverted(ctx context.Context, match pipeline.Match) (pipeline.Alert, bool, error) {
	alert := pipeline.Alert{
		Match:       match,
		Fingerprint: Fingerprint(match),
		Kind:        pipeline.AlertReverted,
		At:          m.now(),
	}
	if err := m.Store.RecordAlert(ctx, alert); err != nil {
		return pipeline.Alert{}, false, err
	}
	return alert, true, nil
}
