package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/adapters"
	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
)

type countingAdapter struct{ dispatches, controls, results atomic.Int32 }
type blockingAdapter struct {
	countingAdapter
	started chan struct{}
	release chan struct{}
}

func (a *blockingAdapter) Dispatch(_ context.Context, r adapters.DispatchRequest) (adapters.Acknowledgement, error) {
	close(a.started)
	<-a.release
	return adapters.Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}

func (a *countingAdapter) Dispatch(_ context.Context, r adapters.DispatchRequest) (adapters.Acknowledgement, error) {
	a.dispatches.Add(1)
	return adapters.Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
func (*countingAdapter) Heartbeat(context.Context, adapters.TaskRef) (adapters.Heartbeat, error) {
	return adapters.Heartbeat{}, nil
}

func (a *countingAdapter) Result(_ context.Context, r adapters.TaskRef) (adapters.Result, error) {
	a.results.Add(1)
	time.Sleep(10 * time.Millisecond)
	return adapters.Result{TaskID: r.TaskID, State: "running"}, nil
}

func TestConcurrentResultPollsAdapterOnce(t *testing.T) {
	store := core.NewStore()
	_ = store.CreateRun(core.Run{ID: "result", Mode: "dispatch"})
	ctrl := dispatch.NewController()
	adapter := &countingAdapter{}
	s := &Server{Mode: "dispatch", Store: store, controllers: map[string]*dispatch.Controller{"result": ctrl}, adapters: map[string]adapters.Adapter{"result": adapter}, runTimeouts: map[string]time.Duration{"result": time.Second}, catalogs: map[string]dispatch.Catalog{}, dags: map[string]dispatch.TaskDAG{}, approvalClaims: map[string]bool{}}
	if err := s.dispatchTask("result", dispatch.Task{ID: "task", Version: "v1"}, "agent"); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runs/{id}/tasks/{task}/result", s.taskResult)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest("POST", "/api/v1/runs/result/tasks/task/result", nil))
		}()
	}
	wg.Wait()
	if got := adapter.results.Load(); got != 1 {
		t.Fatalf("result calls=%d", got)
	}
}

func TestCriticalAppendFailureMakesHealthUnready(t *testing.T) {
	store, err := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	_ = store.CreateRun(core.Run{ID: "health", Mode: "dispatch"})
	_ = store.Close()
	adapter := &countingAdapter{}
	s := &Server{Mode: "dispatch", Store: store, controllers: map[string]*dispatch.Controller{"health": dispatch.NewController()}, adapters: map[string]adapters.Adapter{"health": adapter}, runTimeouts: map[string]time.Duration{"health": time.Second}}
	if err = s.dispatchTask("health", dispatch.Task{ID: "task", Version: "v1"}, "agent"); err == nil {
		t.Fatal("expected append failure")
	}
	if adapter.dispatches.Load() != 0 {
		t.Fatal("adapter ran without durable request evidence")
	}
	w := httptest.NewRecorder()
	s.health(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("health=%d", w.Code)
	}
	called := false
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/test", func(http.ResponseWriter, *http.Request) { called = true })
	guarded := s.security(mux)
	w = httptest.NewRecorder()
	guarded.ServeHTTP(w, httptest.NewRequest("POST", "/api/test", nil))
	if w.Code != http.StatusServiceUnavailable || called {
		t.Fatalf("mutation was not blocked: status=%d called=%v", w.Code, called)
	}
}
func TestTerminalAppendFailureAfterAdapterLatchesHealth(t *testing.T) {
	store, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	_ = store.CreateRun(core.Run{ID: "terminal", Mode: "dispatch"})
	adapter := &blockingAdapter{started: make(chan struct{}), release: make(chan struct{})}
	s := &Server{Mode: "dispatch", Store: store, controllers: map[string]*dispatch.Controller{"terminal": dispatch.NewController()}, adapters: map[string]adapters.Adapter{"terminal": adapter}, runTimeouts: map[string]time.Duration{"terminal": time.Second}}
	done := make(chan error, 1)
	go func() { done <- s.dispatchTask("terminal", dispatch.Task{ID: "task", Version: "v1"}, "agent") }()
	<-adapter.started
	_ = store.Close()
	close(adapter.release)
	if err := <-done; err == nil {
		t.Fatal("expected terminal append failure")
	}
	w := httptest.NewRecorder()
	s.health(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("health=%d", w.Code)
	}
}
func (a *countingAdapter) control(r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	a.controls.Add(1)
	return adapters.Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
func (a *countingAdapter) Pause(_ context.Context, r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	return a.control(r)
}
func (a *countingAdapter) Cancel(_ context.Context, r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	return a.control(r)
}
func (a *countingAdapter) Resume(_ context.Context, r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	return a.control(r)
}
func (a *countingAdapter) Retry(_ context.Context, r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	return a.control(r)
}
func (a *countingAdapter) Reassign(_ context.Context, r adapters.ControlRequest) (adapters.Acknowledgement, error) {
	return a.control(r)
}

func TestConcurrentDispatchAndControlExecuteOnce(t *testing.T) {
	store := core.NewStore()
	_ = store.CreateRun(core.Run{ID: "r", Mode: "dispatch"})
	ctrl := dispatch.NewController()
	adapter := &countingAdapter{}
	s := &Server{Mode: "dispatch", Store: store, catalogs: map[string]dispatch.Catalog{}, dags: map[string]dispatch.TaskDAG{}, controllers: map[string]*dispatch.Controller{"r": ctrl}, adapters: map[string]adapters.Adapter{"r": adapter}, runTimeouts: map[string]time.Duration{"r": time.Second}, approvalClaims: map[string]bool{}}
	task := dispatch.Task{ID: "t", Version: "v1"}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.dispatchTask("r", task, "agent") }()
	}
	wg.Wait()
	if got := adapter.dispatches.Load(); got != 1 {
		t.Fatalf("dispatch calls=%d", got)
	}
	fence := ctrl.RenewLease("control-task")
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runs/{id}/controls/{action}", s.control)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/api/v1/runs/r/controls/pause", strings.NewReader(`{"task_id":"control-task","idempotency_key":"same","fencing_token":`+fmt.Sprint(fence)+`}`))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
		}()
	}
	wg.Wait()
	if got := adapter.controls.Load(); got != 1 {
		t.Fatalf("control calls=%d", got)
	}
}

func TestConcurrentApprovalGrantExecutesOnce(t *testing.T) {
	store := core.NewStore()
	_ = store.CreateRun(core.Run{ID: "approval", Mode: "dispatch"})
	adapter := &countingAdapter{}
	ctrl := dispatch.NewController()
	cat := dispatch.Catalog{Version: "v1", Capabilities: []dispatch.Capability{{Name: "write", Version: "v1", SideEffecting: true}}, Agents: []dispatch.Agent{{ID: "agent", Version: "v1", Capabilities: []string{"write@v1"}, Enabled: true, MaxConcurrent: 1}}}
	dag := dispatch.TaskDAG{Version: "v1", Tasks: []dispatch.Task{{ID: "t", Version: "v1", Requires: []string{"write@v1"}, Cost: 1}}, MaxFanout: 1, Budget: 1}
	s := &Server{Mode: "dispatch", Store: store, catalogs: map[string]dispatch.Catalog{"cat": cat}, dags: map[string]dispatch.TaskDAG{"dag": dag}, controllers: map[string]*dispatch.Controller{"approval": ctrl}, adapters: map[string]adapters.Adapter{"approval": adapter}, runTimeouts: map[string]time.Duration{"approval": time.Second}, approvalClaims: map[string]bool{}}
	_, _ = store.Append("approval", "dispatch.started", map[string]any{"catalog_id": "cat", "dag_id": "dag"})
	action := dispatch.Action{Tool: "write@v1", Target: "t", Arguments: map[string]string{"agent": "agent"}, InputVersions: map[string]string{"task": "v1", "dag": "v1"}, PolicyVersion: "capability/v1", Scope: "dispatch:t"}
	digest := dispatch.ActionDigest(action)
	_, _ = store.Append("approval", "approval.requested", map[string]any{"task_id": "t", "action": action, "action_digest": digest, "expires": time.Now().Add(time.Hour).Format(time.RFC3339), "scope": action.Scope, "authorized_actors": []string{"local-owner"}})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/runs/{id}/approvals/{digest}/{decision}", s.approve)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/api/v1/runs/approval/approvals/"+digest+"/grant", strings.NewReader(`{"actor":"local-owner"}`))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
		}()
	}
	wg.Wait()
	if got := adapter.dispatches.Load(); got != 1 {
		t.Fatalf("dispatch calls=%d", got)
	}
	grants := 0
	for _, e := range store.Events("approval", 0) {
		if e.Type == "approval.granted" {
			grants++
		}
	}
	if grants != 1 {
		t.Fatalf("grant events=%d", grants)
	}
}
