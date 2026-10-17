package redisdiag

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Handler serves diagnostics as JSON. Mount it on an internal/admin port,
// not on the public one: it exposes topology and error messages (never
// passwords).
//
//	GET  <prefix>/            snapshot: options, per-node pool and dial stats
//	GET  <prefix>/events      recent events; ?since=RFC3339 or ?last=50
//	GET  <prefix>/probe       fresh TCP dial + pooled PING + INFO for every node
//	     (POST also accepted)  ?timeout=2s; one probe at a time (409 otherwise)
//
// Example: mux.Handle("/debug/redis/", diag.Handler())
func (in *Instrument) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimSuffix(r.URL.Path, "/")
		switch {
		case strings.HasSuffix(path, "/events"):
			in.serveEvents(w, r)
		case strings.HasSuffix(path, "/probe"):
			in.serveProbe(w, r)
		default:
			writeJSON(w, http.StatusOK, in.Snapshot())
		}
	})
}

func (in *Instrument) serveEvents(w http.ResponseWriter, r *http.Request) {
	ev := in.Events()
	if s := r.URL.Query().Get("since"); s != "" {
		since, err := time.Parse(time.RFC3339, s)
		if err != nil {
			http.Error(w, "since must be RFC3339", http.StatusBadRequest)
			return
		}
		i := 0
		for i < len(ev) && ev[i].Time.Before(since) {
			i++
		}
		ev = ev[i:]
	}
	if s := r.URL.Query().Get("last"); s != "" {
		var n int
		for _, c := range s {
			if c < '0' || c > '9' {
				http.Error(w, "last must be a number", http.StatusBadRequest)
				return
			}
			n = n*10 + int(c-'0')
		}
		if n < len(ev) {
			ev = ev[len(ev)-n:]
		}
	}
	writeJSON(w, http.StatusOK, ev)
}

func (in *Instrument) serveProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST", http.StatusMethodNotAllowed)
		return
	}
	timeout := 2 * time.Second
	if s := r.URL.Query().Get("timeout"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d <= 0 || d > 30*time.Second {
			http.Error(w, "timeout must be a duration in (0, 30s]", http.StatusBadRequest)
			return
		}
		timeout = d
	}
	res, err := in.Probe(r.Context(), timeout)
	if errors.Is(err, ErrProbeRunning) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
