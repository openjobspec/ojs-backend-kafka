package api

import (
	"net/http"

	commonmw "github.com/openjobspec/ojs-go-backend-common/middleware"
)

// KeyAuth returns a middleware that validates Bearer token authentication.
// Requests to paths in skipPaths bypass authentication.
func KeyAuth(apiKey string, skipPaths ...string) func(http.Handler) http.Handler {
	return commonmw.KeyAuth(apiKey, skipPaths...)
}
