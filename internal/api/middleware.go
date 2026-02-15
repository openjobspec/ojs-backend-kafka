package api

import (
	"net/http"
	"strings"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// OJSHeaders middleware adds required OJS response headers.
func OJSHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("OJS-Version", core.OJSVersion)
		w.Header().Set("Content-Type", core.OJSMediaType)

		// Generate or echo X-Request-Id
		reqID := r.Header.Get("X-Request-Id")
		if reqID == "" {
			reqID = "req_" + core.NewUUIDv7()
		}
		w.Header().Set("X-Request-Id", reqID)

		next.ServeHTTP(w, r)
	})
}

// ValidateContentType middleware validates the Content-Type header for POST requests.
func ValidateContentType(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodPatch {
			ct := r.Header.Get("Content-Type")
			if ct != "" {
				mediaType := strings.Split(ct, ";")[0]
				mediaType = strings.TrimSpace(mediaType)
				if mediaType != core.OJSMediaType && mediaType != "application/json" {
					WriteError(w, http.StatusBadRequest, core.NewInvalidRequestError(
						"Unsupported Content-Type. Expected 'application/openjobspec+json' or 'application/json'.",
						map[string]any{
							"received": ct,
						},
					))
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}
