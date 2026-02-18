package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// passthrough is a simple handler that writes a 200 OK response.
var passthrough = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
})

func TestOJSHeaders(t *testing.T) {
	tests := []struct {
		name            string
		requestID       string
		wantVersion     string
		wantContentType string
		wantRequestID   string
		wantReqIDPrefix string
	}{
		{
			name:            "sets OJS-Version header",
			requestID:       "test-id-123",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "test-id-123",
		},
		{
			name:            "sets Content-Type header",
			requestID:       "ct-check",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "ct-check",
		},
		{
			name:            "echoes provided X-Request-Id",
			requestID:       "my-custom-request-id",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantRequestID:   "my-custom-request-id",
		},
		{
			name:            "generates X-Request-Id starting with req_ when none provided",
			requestID:       "",
			wantVersion:     core.OJSVersion,
			wantContentType: core.OJSMediaType,
			wantReqIDPrefix: "req_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.requestID != "" {
				req.Header.Set("X-Request-Id", tt.requestID)
			}
			rr := httptest.NewRecorder()

			handler := OJSHeaders(passthrough)
			handler.ServeHTTP(rr, req)

			if got := rr.Header().Get("OJS-Version"); got != tt.wantVersion {
				t.Errorf("OJS-Version = %q, want %q", got, tt.wantVersion)
			}

			if got := rr.Header().Get("Content-Type"); got != tt.wantContentType {
				t.Errorf("Content-Type = %q, want %q", got, tt.wantContentType)
			}

			gotReqID := rr.Header().Get("X-Request-Id")
			if tt.wantRequestID != "" {
				if gotReqID != tt.wantRequestID {
					t.Errorf("X-Request-Id = %q, want %q", gotReqID, tt.wantRequestID)
				}
			}
			if tt.wantReqIDPrefix != "" {
				if !strings.HasPrefix(gotReqID, tt.wantReqIDPrefix) {
					t.Errorf("X-Request-Id = %q, want prefix %q", gotReqID, tt.wantReqIDPrefix)
				}
				if len(gotReqID) <= len(tt.wantReqIDPrefix) {
					t.Errorf("X-Request-Id = %q is too short; expected prefix %q followed by a UUID", gotReqID, tt.wantReqIDPrefix)
				}
			}
		})
	}
}

func TestValidateContentType(t *testing.T) {
	tests := []struct {
		name        string
		method      string
		contentType string
		wantStatus  int
	}{
		{
			name:        "POST with application/json passes through",
			method:      http.MethodPost,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with application/openjobspec+json passes through",
			method:      http.MethodPost,
			contentType: core.OJSMediaType,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with text/plain returns 400",
			method:      http.MethodPost,
			contentType: "text/plain",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "POST with application/json charset=utf-8 passes through",
			method:      http.MethodPost,
			contentType: "application/json; charset=utf-8",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "POST with no Content-Type passes through",
			method:      http.MethodPost,
			contentType: "",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "GET with invalid Content-Type passes through",
			method:      http.MethodGet,
			contentType: "text/plain",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "PUT with application/json passes through",
			method:      http.MethodPut,
			contentType: "application/json",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "PUT with text/xml returns 400",
			method:      http.MethodPut,
			contentType: "text/xml",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "PATCH with application/openjobspec+json passes through",
			method:      http.MethodPatch,
			contentType: core.OJSMediaType,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "DELETE with invalid Content-Type passes through",
			method:      http.MethodDelete,
			contentType: "text/plain",
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/", nil)
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}
			rr := httptest.NewRecorder()

			handler := ValidateContentType(passthrough)
			handler.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d", rr.Code, tt.wantStatus)
			}

			if tt.wantStatus == http.StatusBadRequest {
				body := rr.Body.String()
				if !strings.Contains(body, "invalid_request") {
					t.Errorf("expected error body to contain %q, got %q", "invalid_request", body)
				}
				if !strings.Contains(body, "Unsupported Content-Type") {
					t.Errorf("expected error body to contain %q, got %q", "Unsupported Content-Type", body)
				}
			}
		})
	}
}

// Note: LimitRequestBody is not exposed by this backend's middleware package.
