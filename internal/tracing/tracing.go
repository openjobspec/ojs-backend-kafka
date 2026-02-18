// Package tracing provides distributed tracing utilities for the OJS Kafka backend.
// It re-exports the shared tracing package from ojs-go-backend-common and adds
// a Setup function for initializing OpenTelemetry with OTLP gRPC export.
package tracing

import (
	"context"
	"os"

	ojsotel "github.com/openjobspec/ojs-go-backend-common/otel"
	"github.com/openjobspec/ojs-go-backend-common/tracing"
)

// Re-export shared tracing utilities.
var (
	StartSpan         = tracing.StartSpan
	StartSpanWithKind = tracing.StartSpanWithKind
	StartConsumerSpan = tracing.StartConsumerSpan
	RecordError       = tracing.RecordError
	SetOK             = tracing.SetOK
	InjectContext     = tracing.InjectContext
	ExtractContext    = tracing.ExtractContext
	InjectContextJSON = tracing.InjectContextJSON
	ExtractContextJSON = tracing.ExtractContextJSON
	JobID             = tracing.JobID
	JobType           = tracing.JobType
	JobQueue          = tracing.JobQueue
	JobAttempt        = tracing.JobAttempt
	Tracer            = tracing.Tracer
)

// Setup initializes OpenTelemetry tracing for the backend.
// It reads OTEL_EXPORTER_OTLP_ENDPOINT (standard) and OJS_OTEL_ENDPOINT (override).
// Returns a shutdown function. No-op if neither env var is set.
func Setup(serviceName string) (func(), error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if ep := os.Getenv("OJS_OTEL_ENDPOINT"); ep != "" {
		endpoint = ep
	}

	enabled := os.Getenv("OJS_OTEL_ENABLED") == "true" || endpoint != ""

	shutdown, err := ojsotel.Init(context.Background(), ojsotel.Config{
		ServiceName: serviceName,
		Enabled:     enabled,
		Endpoint:    endpoint,
	})
	if err != nil {
		return nil, err
	}

	return func() { _ = shutdown(context.Background()) }, nil
}
