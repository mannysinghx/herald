package server

import (
	_ "embed"
	"net/http"
)

//go:embed page.html
var pageHTML []byte

// handlePage serves the small built-in page: a topic picker and a live view of one topic.
func (s *Server) handlePage(w http.ResponseWriter, _ *http.Request, _ *request) *apiError {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Content-Security-Policy", "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'")
	_, _ = w.Write(pageHTML)
	return nil
}

// handleTopicPage answers GET /<topic>: the UnifiedPush discovery probe, or the page.
func (s *Server) handleTopicPage(w http.ResponseWriter, r *http.Request, q *request) *apiError {
	if boolParam(r, false, "x-unifiedpush", "unifiedpush", "up") {
		return writeJSON(w, map[string]any{"unifiedpush": map[string]int{"version": 1}})
	}
	return s.handlePage(w, r, q)
}
