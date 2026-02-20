package api

import (
	"net/http"

	commonapi "github.com/openjobspec/ojs-go-backend-common/api"
	"github.com/openjobspec/ojs-go-backend-common/core"
)

// SystemHandler handles system-related HTTP endpoints.
// Wraps the shared SystemHandler with Kafka-specific manifest config.
type SystemHandler = commonapi.SystemHandler

// NewSystemHandler creates a new SystemHandler with Kafka-specific manifest.
func NewSystemHandler(backend core.Backend) *SystemHandler {
	return commonapi.NewSystemHandler(backend, commonapi.ManifestConfig{
		ImplementationName: "ojs-backend-kafka",
		ImplementationVer:  core.OJSVersion,
		BackendName:        "kafka",
		ConformanceLevel:   4,
		Capabilities: map[string]any{
			"batch_enqueue":     true,
			"cron_jobs":         true,
			"dead_letter":       true,
			"delayed_jobs":      true,
			"job_ttl":           true,
			"priority_queues":   true,
			"rate_limiting":     false,
			"schema_validation": true,
			"unique_jobs":       true,
			"workflows":         true,
			"pause_resume":      true,
		},
		Extensions: map[string]any{
			"official": []map[string]any{
				{"name": "admin-api", "uri": "urn:ojs:ext:admin-api", "version": "1.0.0"},
				{"name": "dead-letter", "uri": "urn:ojs:ext:dead-letter", "version": "1.0.0"},
			},
		},
	})
}

// Ensure the Manifest and Health methods are accessible.
var (
	_ func(http.ResponseWriter, *http.Request) = (*SystemHandler)(nil).Manifest
	_ func(http.ResponseWriter, *http.Request) = (*SystemHandler)(nil).Health
)
