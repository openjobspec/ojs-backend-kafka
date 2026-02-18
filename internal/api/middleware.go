package api

import (
	"net/http"

	commonmw "github.com/openjobspec/ojs-go-backend-common/middleware"
)

// OJSHeaders middleware adds required OJS response headers.
func OJSHeaders(next http.Handler) http.Handler {
	return commonmw.OJSHeaders(next)
}

// ValidateContentType middleware validates the Content-Type header for mutation requests.
func ValidateContentType(next http.Handler) http.Handler {
	return commonmw.ValidateContentType(next)
}
