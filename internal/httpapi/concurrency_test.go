package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/adapters"
	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
)

type countingAdapter struct{ dispatches, controls atomic.Int32 }

func (a *countingAdapter) Dispatch(_ context.Context, r adapters.DispatchRequest) (adapters.Acknowledgement, error) {
	a.dispatches.Add(1)
	return adapters.Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
func (*countingAdapter) Heartbeat(context.Context, adapters.TaskRef) (adapters.Heartbeat, error) {
	return adapters.Heartbeat{}, nil
}
func (*countingAdapter) Result(_ context.Context, r adapters.TaskRef) (adapters.Result, error) {
	return adapters.Result{TaskID: r.TaskID, State: "running"}, nil
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
