package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openjobspec/ojs-backend-kafka/internal/core"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ojsv1 "github.com/openjobspec/ojs-proto/gen/go/ojs/v1"
)

// Server implements the OJSService gRPC service by delegating to a core.Backend.
type Server struct {
	ojsv1.UnimplementedOJSServiceServer
	backend core.Backend
}

// Register creates a new gRPC OJS server and registers it with the given gRPC server.
func Register(s *grpc.Server, backend core.Backend) {
	ojsv1.RegisterOJSServiceServer(s, &Server{backend: backend})
}

// New returns a new gRPC OJS server wrapping the given backend.
func New(backend core.Backend) *Server {
	return &Server{backend: backend}
}

// --- System RPCs ---

func (s *Server) Manifest(ctx context.Context, req *ojsv1.ManifestRequest) (*ojsv1.ManifestResponse, error) {
	return &ojsv1.ManifestResponse{
		OjsVersion:       "1.0.0-rc.1",
		ConformanceLevel: 4,
		Protocols:        []string{"http", "grpc"},
		Backend:          "kafka",
		Implementation: &ojsv1.Implementation{
			Name:     "ojs-backend-kafka",
			Version:  "1.0.0",
			Language: "go",
		},
	}, nil
}

func (s *Server) Health(ctx context.Context, req *ojsv1.HealthRequest) (*ojsv1.HealthResponse, error) {
	h, err := s.backend.Health(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "health check failed: %v", err)
	}

	st := ojsv1.HealthStatus_HEALTH_STATUS_OK
	if h.Backend.Status != "ok" {
		st = ojsv1.HealthStatus_HEALTH_STATUS_DEGRADED
	}

	return &ojsv1.HealthResponse{
		Status:    st,
		Timestamp: timestamppb.Now(),
		Details:   mustStruct(map[string]any{"backend": h.Backend.Type, "latency_ms": h.Backend.LatencyMs}),
	}, nil
}

// --- Job RPCs ---

func (s *Server) Enqueue(ctx context.Context, req *ojsv1.EnqueueRequest) (*ojsv1.EnqueueResponse, error) {
	job := enqueueRequestToJob(req)

	result, err := s.backend.Push(ctx, job)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.EnqueueResponse{
		Job: jobToProto(result),
	}, nil
}

func (s *Server) EnqueueBatch(ctx context.Context, req *ojsv1.EnqueueBatchRequest) (*ojsv1.EnqueueBatchResponse, error) {
	jobs := make([]*core.Job, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		jobs = append(jobs, enqueueJobRequestToJob(j))
	}

	results, err := s.backend.PushBatch(ctx, jobs)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	protoJobs := make([]*ojsv1.Job, 0, len(results))
	for _, r := range results {
		protoJobs = append(protoJobs, jobToProto(r))
	}

	return &ojsv1.EnqueueBatchResponse{
		Jobs: protoJobs,
	}, nil
}

func (s *Server) GetJob(ctx context.Context, req *ojsv1.GetJobRequest) (*ojsv1.GetJobResponse, error) {
	job, err := s.backend.Info(ctx, req.JobId)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.GetJobResponse{
		Job: jobToProto(job),
	}, nil
}

func (s *Server) CancelJob(ctx context.Context, req *ojsv1.CancelJobRequest) (*ojsv1.CancelJobResponse, error) {
	job, err := s.backend.Cancel(ctx, req.JobId)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.CancelJobResponse{
		Job: jobToProto(job),
	}, nil
}

// --- Worker RPCs ---

func (s *Server) Fetch(ctx context.Context, req *ojsv1.FetchRequest) (*ojsv1.FetchResponse, error) {
	count := int(req.Count)
	if count <= 0 {
		count = 1
	}

	visibilityMs := 30000

	jobs, err := s.backend.Fetch(ctx, req.Queues, count, req.WorkerId, visibilityMs)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	protoJobs := make([]*ojsv1.Job, 0, len(jobs))
	for _, j := range jobs {
		protoJobs = append(protoJobs, jobToProto(j))
	}

	return &ojsv1.FetchResponse{
		Jobs: protoJobs,
	}, nil
}

func (s *Server) Ack(ctx context.Context, req *ojsv1.AckRequest) (*ojsv1.AckResponse, error) {
	var result []byte
	if req.Result != nil {
		var err error
		result, err = json.Marshal(req.Result.AsMap())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid result: %v", err)
		}
	}

	ackResp, err := s.backend.Ack(ctx, req.JobId, result)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.AckResponse{
		Acknowledged: ackResp.Acknowledged,
	}, nil
}

func (s *Server) Nack(ctx context.Context, req *ojsv1.NackRequest) (*ojsv1.NackResponse, error) {
	jobErr := &core.JobError{
		Message: req.Error.GetMessage(),
		Type:    req.Error.GetCode(),
	}

	nackResp, err := s.backend.Nack(ctx, req.JobId, jobErr, false)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.NackResponse{
		State: stateToProto[nackResp.State],
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *ojsv1.HeartbeatRequest) (*ojsv1.HeartbeatResponse, error) {
	visibilityMs := 30000

	hbResp, err := s.backend.Heartbeat(ctx, req.WorkerId, nil, visibilityMs)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	_ = hbResp
	return &ojsv1.HeartbeatResponse{}, nil
}

// --- Queue RPCs ---

func (s *Server) ListQueues(ctx context.Context, req *ojsv1.ListQueuesRequest) (*ojsv1.ListQueuesResponse, error) {
	queues, err := s.backend.ListQueues(ctx)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	protoQueues := make([]*ojsv1.QueueInfo, 0, len(queues))
	for _, q := range queues {
		protoQueues = append(protoQueues, &ojsv1.QueueInfo{
			Name:   q.Name,
			Paused: q.Status == "paused",
		})
	}

	return &ojsv1.ListQueuesResponse{
		Queues: protoQueues,
	}, nil
}

func (s *Server) QueueStats(ctx context.Context, req *ojsv1.QueueStatsRequest) (*ojsv1.QueueStatsResponse, error) {
	stats, err := s.backend.QueueStats(ctx, req.Queue)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.QueueStatsResponse{
		Queue: stats.Queue,
		Stats: &ojsv1.QueueStatistics{
			Available: int64(stats.Stats.Available),
			Active:    int64(stats.Stats.Active),
			Scheduled: int64(stats.Stats.Scheduled),
			Retryable: int64(stats.Stats.Retryable),
		},
	}, nil
}

func (s *Server) PauseQueue(ctx context.Context, req *ojsv1.PauseQueueRequest) (*ojsv1.PauseQueueResponse, error) {
	if err := s.backend.PauseQueue(ctx, req.Queue); err != nil {
		return nil, coreErrorToGRPC(err)
	}
	return &ojsv1.PauseQueueResponse{}, nil
}

func (s *Server) ResumeQueue(ctx context.Context, req *ojsv1.ResumeQueueRequest) (*ojsv1.ResumeQueueResponse, error) {
	if err := s.backend.ResumeQueue(ctx, req.Queue); err != nil {
		return nil, coreErrorToGRPC(err)
	}
	return &ojsv1.ResumeQueueResponse{}, nil
}

// --- Dead Letter RPCs ---

func (s *Server) ListDeadLetter(ctx context.Context, req *ojsv1.ListDeadLetterRequest) (*ojsv1.ListDeadLetterResponse, error) {
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 25
	}

	jobs, total, err := s.backend.ListDeadLetter(ctx, limit, 0)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	protoJobs := make([]*ojsv1.Job, 0, len(jobs))
	for _, j := range jobs {
		protoJobs = append(protoJobs, jobToProto(j))
	}

	return &ojsv1.ListDeadLetterResponse{
		Jobs:       protoJobs,
		TotalCount: int64(total),
	}, nil
}

func (s *Server) RetryDeadLetter(ctx context.Context, req *ojsv1.RetryDeadLetterRequest) (*ojsv1.RetryDeadLetterResponse, error) {
	job, err := s.backend.RetryDeadLetter(ctx, req.JobId)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.RetryDeadLetterResponse{
		Job: jobToProto(job),
	}, nil
}

func (s *Server) DeleteDeadLetter(ctx context.Context, req *ojsv1.DeleteDeadLetterRequest) (*ojsv1.DeleteDeadLetterResponse, error) {
	if err := s.backend.DeleteDeadLetter(ctx, req.JobId); err != nil {
		return nil, coreErrorToGRPC(err)
	}
	return &ojsv1.DeleteDeadLetterResponse{}, nil
}

// --- Cron RPCs ---

func (s *Server) RegisterCron(ctx context.Context, req *ojsv1.RegisterCronRequest) (*ojsv1.RegisterCronResponse, error) {
	argsJSON, _ := json.Marshal(valuesToInterface(req.Args))

	cronJob := &core.CronJob{
		Name:       req.Name,
		Expression: req.Cron,
		Timezone:   req.Timezone,
		Enabled:    true,
		JobTemplate: &core.CronJobTemplate{
			Type: req.Type,
			Args: argsJSON,
		},
	}

	result, err := s.backend.RegisterCron(ctx, cronJob)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	resp := &ojsv1.RegisterCronResponse{
		Name: result.Name,
	}
	if result.NextRunAt != "" {
		if t, err := time.Parse(time.RFC3339, result.NextRunAt); err == nil {
			resp.NextRunAt = timestamppb.New(t)
		}
	}
	return resp, nil
}

func (s *Server) UnregisterCron(ctx context.Context, req *ojsv1.UnregisterCronRequest) (*ojsv1.UnregisterCronResponse, error) {
	if _, err := s.backend.DeleteCron(ctx, req.Name); err != nil {
		return nil, coreErrorToGRPC(err)
	}
	return &ojsv1.UnregisterCronResponse{}, nil
}

func (s *Server) ListCron(ctx context.Context, req *ojsv1.ListCronRequest) (*ojsv1.ListCronResponse, error) {
	crons, err := s.backend.ListCron(ctx)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	entries := make([]*ojsv1.CronEntry, 0, len(crons))
	for _, c := range crons {
		entry := &ojsv1.CronEntry{
			Name:     c.Name,
			Cron:     c.Expression,
			Timezone: c.Timezone,
		}
		if c.JobTemplate != nil {
			entry.Type = c.JobTemplate.Type
		}
		if c.NextRunAt != "" {
			if t, err := time.Parse(time.RFC3339, c.NextRunAt); err == nil {
				entry.NextRunAt = timestamppb.New(t)
			}
		}
		if c.LastRunAt != "" {
			if t, err := time.Parse(time.RFC3339, c.LastRunAt); err == nil {
				entry.LastRunAt = timestamppb.New(t)
			}
		}
		entries = append(entries, entry)
	}

	return &ojsv1.ListCronResponse{
		Entries: entries,
	}, nil
}

// --- Workflow RPCs ---

func (s *Server) CreateWorkflow(ctx context.Context, req *ojsv1.CreateWorkflowRequest) (*ojsv1.CreateWorkflowResponse, error) {
	wfReq := protoToWorkflowRequest(req)

	wf, err := s.backend.CreateWorkflow(ctx, wfReq)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.CreateWorkflowResponse{
		Workflow: workflowToProto(wf),
	}, nil
}

func (s *Server) GetWorkflow(ctx context.Context, req *ojsv1.GetWorkflowRequest) (*ojsv1.GetWorkflowResponse, error) {
	wf, err := s.backend.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.GetWorkflowResponse{
		Workflow: workflowToProto(wf),
	}, nil
}

func (s *Server) CancelWorkflow(ctx context.Context, req *ojsv1.CancelWorkflowRequest) (*ojsv1.CancelWorkflowResponse, error) {
	wf, err := s.backend.CancelWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, coreErrorToGRPC(err)
	}

	return &ojsv1.CancelWorkflowResponse{
		Workflow: workflowToProto(wf),
	}, nil
}

// --- Streaming RPCs (optional, return UNIMPLEMENTED by default) ---

func (s *Server) StreamJobs(req *ojsv1.StreamJobsRequest, stream ojsv1.OJSService_StreamJobsServer) error {
	return status.Errorf(codes.Unimplemented, "StreamJobs not yet implemented")
}

func (s *Server) StreamEvents(req *ojsv1.StreamEventsRequest, stream ojsv1.OJSService_StreamEventsServer) error {
	return status.Errorf(codes.Unimplemented, "StreamEvents not yet implemented")
}

// --- Helpers ---

func mustStruct(m map[string]any) *structpb.Struct {
	s, _ := structpb.NewStruct(m)
	return s
}

func coreErrorToGRPC(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()

	switch {
	case isNotFound(msg):
		return status.Errorf(codes.NotFound, "%s", msg)
	case isConflict(msg):
		return status.Errorf(codes.AlreadyExists, "%s", msg)
	case isDuplicate(msg):
		return status.Errorf(codes.AlreadyExists, "%s", msg)
	case isValidation(msg):
		return status.Errorf(codes.InvalidArgument, "%s", msg)
	default:
		return status.Errorf(codes.Internal, "%s", msg)
	}
}

func isNotFound(msg string) bool {
	return contains(msg, "not found") || contains(msg, "not_found")
}

func isConflict(msg string) bool {
	return contains(msg, "conflict") || contains(msg, "invalid state transition")
}

func isDuplicate(msg string) bool {
	return contains(msg, "duplicate") || contains(msg, "already exists")
}

func isValidation(msg string) bool {
	return contains(msg, "invalid") || contains(msg, "required") || contains(msg, "validation")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func valuesToInterface(vals []*structpb.Value) []any {
	result := make([]any, 0, len(vals))
	for _, v := range vals {
		result = append(result, v.AsInterface())
	}
	return result
}

func parseRFC3339(s string) *timestamppb.Timestamp {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return timestamppb.New(t)
}

func intPtr(v int) *int { return &v }

// Suppress unused import warning for fmt
var _ = fmt.Sprintf
