package httpapi

import (
	"net/http"

	"github.com/nehemiyawicks/opsentry/internal/obs"
)

func New() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("/metrics", obs.Handler())
	return mux
}
