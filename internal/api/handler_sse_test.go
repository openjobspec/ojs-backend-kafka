package api

import (
"bufio"
"context"
"net/http"
"net/http/httptest"
"strings"
"testing"
"time"

"github.com/go-chi/chi/v5"

"github.com/openjobspec/ojs-backend-kafka/internal/core"
)

// mockSubscriber implements core.EventSubscriber for testing.
type mockSubscriber struct {
jobCh   chan *core.JobEvent
queueCh chan *core.JobEvent
allCh   chan *core.JobEvent
}

func newMockSubscriber() *mockSubscriber {
return &mockSubscriber{
jobCh:   make(chan *core.JobEvent, 10),
queueCh: make(chan *core.JobEvent, 10),
allCh:   make(chan *core.JobEvent, 10),
}
}

func (m *mockSubscriber) SubscribeJob(jobID string) (<-chan *core.JobEvent, func(), error) {
return m.jobCh, func() {}, nil
}

func (m *mockSubscriber) SubscribeQueue(queue string) (<-chan *core.JobEvent, func(), error) {
return m.queueCh, func() {}, nil
}

func (m *mockSubscriber) SubscribeAll() (<-chan *core.JobEvent, func(), error) {
return m.allCh, func() {}, nil
}

func TestSSEJobEvents_Success(t *testing.T) {
backend := &mockBackend{
infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
return &core.Job{ID: jobID, Type: "email.send", State: "active", Queue: "default"}, nil
},
}
sub := newMockSubscriber()
handler := NewSSEHandler(backend, sub)

r := chi.NewRouter()
r.Get("/ojs/v1/jobs/{id}/events", handler.JobEvents)

// Pre-load events into the buffered channel, then close to signal end-of-stream.
sub.jobCh <- core.NewStateChangedEvent("job-123", "default", "email.send", "available", "active")
close(sub.jobCh)

req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/job-123/events", nil)
req.Header.Set("Accept", "text/event-stream")
w := httptest.NewRecorder()

ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
defer cancel()
req = req.WithContext(ctx)

r.ServeHTTP(w, req)

if w.Header().Get("Content-Type") != "text/event-stream" {
t.Errorf("Content-Type: got %s, want text/event-stream", w.Header().Get("Content-Type"))
}

body := w.Body.String()
if !strings.Contains(body, "retry: 3000") {
t.Error("expected retry field in SSE stream")
}
if !strings.Contains(body, "event: job.state_changed") {
t.Errorf("expected job.state_changed event, got: %s", body)
}
if !strings.Contains(body, `"job_id":"job-123"`) {
t.Error("expected job_id in event data")
}
}

func TestSSEJobEvents_NotFound(t *testing.T) {
backend := &mockBackend{
infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
return nil, core.NewNotFoundError("Job", jobID)
},
}
sub := newMockSubscriber()
handler := NewSSEHandler(backend, sub)

r := chi.NewRouter()
r.Get("/ojs/v1/jobs/{id}/events", handler.JobEvents)

req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/nonexistent/events", nil)
w := httptest.NewRecorder()
r.ServeHTTP(w, req)

if w.Code != http.StatusNotFound {
t.Fatalf("expected 404, got %d", w.Code)
}
}

func TestSSEJobEvents_TerminalState(t *testing.T) {
backend := &mockBackend{
infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
return &core.Job{ID: jobID, Type: "test", State: "completed", Queue: "default"}, nil
},
}
sub := newMockSubscriber()
handler := NewSSEHandler(backend, sub)

r := chi.NewRouter()
r.Get("/ojs/v1/jobs/{id}/events", handler.JobEvents)

req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/job-123/events", nil)
w := httptest.NewRecorder()
r.ServeHTTP(w, req)

if w.Header().Get("Content-Type") != "text/event-stream" {
t.Errorf("Content-Type: got %s, want text/event-stream", w.Header().Get("Content-Type"))
}

body := w.Body.String()
if !strings.Contains(body, "event: job.state_changed") {
t.Errorf("expected terminal state event, got: %s", body)
}
}

func TestSSEQueueEvents_Success(t *testing.T) {
backend := &mockBackend{
queueStatsFunc: func(ctx context.Context, name string) (*core.QueueStats, error) {
return &core.QueueStats{Queue: name, Status: "active"}, nil
},
}
sub := newMockSubscriber()
handler := NewSSEHandler(backend, sub)

r := chi.NewRouter()
r.Get("/ojs/v1/queues/{name}/events", handler.QueueEvents)

// Pre-load event into buffered channel.
sub.queueCh <- core.NewStateChangedEvent("job-456", "default", "email.send", "active", "completed")
close(sub.queueCh)

req := httptest.NewRequest(http.MethodGet, "/ojs/v1/queues/default/events", nil)
req.Header.Set("Accept", "text/event-stream")

ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
defer cancel()
req = req.WithContext(ctx)

w := httptest.NewRecorder()
r.ServeHTTP(w, req)

body := w.Body.String()
if !strings.Contains(body, "event: job.state_changed") {
t.Errorf("expected event in stream, got: %s", body)
}
}

func TestSSEHeartbeat(t *testing.T) {
backend := &mockBackend{
infoFunc: func(ctx context.Context, jobID string) (*core.Job, error) {
return &core.Job{ID: jobID, Type: "test", State: "active", Queue: "default"}, nil
},
}
sub := newMockSubscriber()
handler := NewSSEHandler(backend, sub)

r := chi.NewRouter()
r.Get("/ojs/v1/jobs/{id}/events", handler.JobEvents)

// Pre-load event, then close to end stream.
sub.jobCh <- core.NewStateChangedEvent("job-789", "default", "test", "available", "active")
close(sub.jobCh)

req := httptest.NewRequest(http.MethodGet, "/ojs/v1/jobs/job-789/events", nil)
ctx, cancel := context.WithTimeout(req.Context(), 2*time.Second)
defer cancel()
req = req.WithContext(ctx)

w := httptest.NewRecorder()
r.ServeHTTP(w, req)

body := w.Body.String()
scanner := bufio.NewScanner(strings.NewReader(body))
foundID := false
foundEvent := false
foundData := false
for scanner.Scan() {
line := scanner.Text()
if strings.HasPrefix(line, "id: ") {
foundID = true
}
if strings.HasPrefix(line, "event: ") {
foundEvent = true
}
if strings.HasPrefix(line, "data: ") {
foundData = true
}
}
if !foundID {
t.Error("SSE event missing id field")
}
if !foundEvent {
t.Error("SSE event missing event field")
}
if !foundData {
t.Error("SSE event missing data field")
}
}
