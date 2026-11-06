// Package web serves the browser UI for submitting and viewing forms.
package web

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Register mounts the UI at the site root.
func Register(mux *http.ServeMux) {
	// "{$}" matches "/" exactly, so unknown paths still 404.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
}
