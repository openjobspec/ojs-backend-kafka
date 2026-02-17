package api

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// KeyAuth returns a middleware that validates Bearer token authentication.
// Requests to paths in skipPaths bypass authentication.
func KeyAuth(apiKey string, skipPaths ...string) func(http.Handler) http.Handler {
	skip := make(map[string]bool, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = true
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skip[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			auth := r.Header.Get("Authorization")
			if auth == "" {
				WriteError(w, http.StatusUnauthorized, &core.OJSError{
					Code:    "unauthorized",
					Message: "Missing Authorization header. Expected 'Bearer <api_key>'.",
				})
				return
			}

			token := strings.TrimPrefix(auth, "Bearer ")
			if token == auth {
				WriteError(w, http.StatusUnauthorized, &core.OJSError{
					Code:    "unauthorized",
					Message: "Invalid Authorization format. Expected 'Bearer <api_key>'.",
				})
				return
			}

			if subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) != 1 {
				WriteError(w, http.StatusForbidden, &core.OJSError{
					Code:    "forbidden",
					Message: "Invalid API key.",
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
