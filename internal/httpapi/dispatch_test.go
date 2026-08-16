package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
)

func call(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func seedDefinitions(t *testing.T, h http.Handler) (string, string) {
	t.Helper()
	cat := `{"id":"cat","catalog":{"Version":"v1","Capabilities":[{"Name":"research","Version":"v1","SideEffecting":false}],"Agents":[{"ID":"scout","Version":"v1","Capabilities":["research@v1"],"Priority":10,"Enabled":true,"MaxConcurrent":1}]}}`
	w := call(t, h, "POST", "/api/v1/agents", cat)
	if w.Code != 201 {
		t.Fatalf("catalog: %d %s", w.Code, w.Body.String())
	}
	dag := `{"id":"dag","dag":{"Version":"v1","Tasks":[{"ID":"task-1","Version":"v1","Requires":["research@v1"],"Cost":1}],"MaxFanout":2,"Budget":2}}`
	w = call(t, h, "POST", "/api/v1/tasks", dag)
	if w.Code != 201 {
		t.Fatalf("dag: %d %s", w.Code, w.Body.String())
	}
	return "cat", "dag"
}

func TestDispatchPassPersistsAndControls(t *testing.T) {
	db := filepath.Join(t.TempDir(), "d.db")
	s, err := core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	h := New("dispatch", s)
	cat, dag := seedDefinitions(t, h)
	w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"pass","name":"pass","catalog_id":"`+cat+`","dag_id":"`+dag+`","adapter":"fixture","fixture_mode":"pass"}`)
	if w.Code != 201 {
		t.Fatalf("run: %d %s", w.Code, w.Body.String())
	}
	events := s.Events("pass", 0)
	seen := map[string]bool{}
	for _, e := range events {
		seen[e.Type] = true
	}
	for _, typ := range []string{"dispatch.started", "routing.decided", "command.requested", "command.acknowledged", "task.delegated", "command.completed", "dispatch.completed"} {
		if !seen[typ] {
			t.Fatalf("missing %s", typ)
		}
	}
	w = call(t, h, "POST", "/api/v1/runs/pass/controls/pause", `{"task_id":"task-1","idempotency_key":"pause-1"}`)
	if w.Code != 202 {
		t.Fatalf("pause %d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/pass/controls/pause", `{"task_id":"task-1","idempotency_key":"pause-1"}`)
	if w.Code != 200 {
		t.Fatalf("idempotent duplicate did not return prior result: %d", w.Code)
	}
	_ = s.Close()
	s, err = core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	defs, err := s.Definitions("catalog")
	if err != nil || defs["cat"] == nil {
		t.Fatalf("definition not durable: %v", err)
	}
}

func TestLostResponseRequiresReconciliation(t *testing.T) {
	s, err := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"unknown","catalog_id":"cat","dag_id":"dag","adapter":"fixture","fixture_mode":"lost_response"}`)
	if w.Code != 201 {
		t.Fatalf("run: %d %s", w.Code, w.Body.String())
	}
	raw, _ := json.Marshal(s.Events("unknown", 0))
	if !strings.Contains(string(raw), "command.outcome_unknown") || !strings.Contains(string(raw), "reconciliation_required") || strings.Contains(string(raw), "command.completed") {
		t.Fatalf("unsafe unknown outcome: %s", raw)
	}
	w = call(t, h, "POST", "/api/v1/runs/unknown/reconcile", `{"idempotency_key":"unknown:task-1:dispatch","result":"observed-not-applied"}`)
	if w.Code != 202 {
		t.Fatalf("reconcile: %d %s", w.Code, w.Body.String())
	}
}

func TestRestartMarksRunningDispatchInterrupted(t *testing.T) {
	db := filepath.Join(t.TempDir(), "d.db")
	s, err := core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.CreateRun(core.Run{ID: "r", Mode: "dispatch"})
	_, _ = s.Append("r", "dispatch.started", map[string]any{})
	_ = s.Close()
	s, err = core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = New("dispatch", s)
	p, _ := s.Replay("r", 0)
	if p.Status != "interrupted" {
		t.Fatalf("status=%s", p.Status)
	}
}

func TestMutationSecurity(t *testing.T) {
	s := core.NewStore()
	h := NewAuthenticated("dispatch", s, "token")
	r := httptest.NewRequest("POST", "http://host/api/v1/runs", strings.NewReader(`{"id":"x"}`))
	r.Host = "host"
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set("Origin", "https://evil.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-origin=%d", w.Code)
	}
	w = call(t, h, "POST", "/api/v1/runs/x/events", `{"type":"dispatch.completed"}`)
	if w.Code != http.StatusUnauthorized && w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("event injection exposed: %d", w.Code)
	}
}
