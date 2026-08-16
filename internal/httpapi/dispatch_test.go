package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
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
	for _, typ := range []string{"dispatch.started", "routing.decided", "command.requested", "command.acknowledged", "task.delegated"} {
		if !seen[typ] {
			t.Fatalf("missing %s", typ)
		}
	}
	if seen["command.completed"] || seen["dispatch.completed"] {
		t.Fatal("acknowledgement was incorrectly treated as a result")
	}
	w = call(t, h, "POST", "/api/v1/runs/pass/controls/pause", `{"task_id":"task-1","idempotency_key":"pause-1","fencing_token":1}`)
	if w.Code != 202 {
		t.Fatalf("pause %d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/pass/controls/pause", `{"task_id":"task-1","idempotency_key":"pause-1","fencing_token":1}`)
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
	h = New("dispatch", s)
	w = call(t, h, "POST", "/api/v1/runs/pass/controls/pause", `{"task_id":"task-1","idempotency_key":"pause-1"}`)
	if w.Code != 200 {
		t.Fatalf("persisted duplicate after restart=%d %s", w.Code, w.Body.String())
	}
}

func TestApprovalBindsCanonicalActionExpiryScopeAndActor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  bool
		expired bool
		actor   string
		want    int
	}{
		{"valid", false, false, "owner", 202}, {"mutated", true, false, "owner", 403},
		{"expired", false, true, "owner", 403}, {"unauthorized", false, false, "intruder", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := core.NewStore()
			_ = s.CreateRun(core.Run{ID: "approval", Mode: "dispatch"})
			h := New("dispatch", s)
			a := dispatch.Action{Tool: "release", Target: "prod", Arguments: map[string]string{"version": "1"}, PolicyVersion: "v1", Scope: "release"}
			digest := dispatch.ActionDigest(a)
			if tc.mutate {
				a.Arguments["version"] = "2"
			}
			expires := time.Now().Add(time.Hour)
			if tc.expired {
				expires = time.Now().Add(-time.Hour)
			}
			_, _ = s.Append("approval", "approval.requested", map[string]any{"action": a, "action_digest": digest, "expires": expires.Format(time.RFC3339), "scope": "release", "authorized_actors": []string{"owner"}})
			w := call(t, h, "POST", "/api/v1/runs/approval/approvals/"+digest+"/grant", `{"actor":"`+tc.actor+`"}`)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestTopologicalOrdersChainsWithEqualDependencyCounts(t *testing.T) {
	tasks := []dispatch.Task{{ID: "d", DependsOn: []string{"c"}}, {ID: "b", DependsOn: []string{"a"}}, {ID: "c", DependsOn: []string{"b"}}, {ID: "a"}}
	got, err := topological(tasks)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, task := range got {
		ids = append(ids, task.ID)
	}
	if strings.Join(ids, ",") != "a,b,c,d" {
		t.Fatalf("order=%v", ids)
	}
}

func TestSideEffectingTaskRequestsValidApprovalBeforeDispatch(t *testing.T) {
	s, err := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := New("dispatch", s)
	cat := `{"id":"side","catalog":{"Version":"v1","Capabilities":[{"Name":"write","Version":"v1","SideEffecting":true}],"Agents":[{"ID":"forge","Version":"v1","Capabilities":["write@v1"],"Priority":10,"Enabled":true,"MaxConcurrent":1}]}}`
	if w := call(t, h, "POST", "/api/v1/agents", cat); w.Code != 201 {
		t.Fatalf("catalog=%d %s", w.Code, w.Body.String())
	}
	dag := `{"id":"side-dag","dag":{"Version":"v1","Tasks":[{"ID":"publish","Version":"v1","Requires":["write@v1"],"Cost":1}],"MaxFanout":1,"Budget":1}}`
	if w := call(t, h, "POST", "/api/v1/tasks", dag); w.Code != 201 {
		t.Fatalf("dag=%d %s", w.Code, w.Body.String())
	}
	w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"side-run","catalog_id":"side","dag_id":"side-dag","adapter":"fixture"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("run=%d %s", w.Code, w.Body.String())
	}
	var request core.Event
	for _, event := range s.Events("side-run", 0) {
		if event.Type == "approval.requested" {
			request = event
		}
		if event.Type == "command.requested" {
			t.Fatal("side effect dispatched before approval")
		}
	}
	digest := stringValue(request.Data["action_digest"])
	if len(digest) != 64 {
		t.Fatalf("invalid digest %q", digest)
	}
	w = call(t, h, "POST", "/api/v1/runs/side-run/approvals/"+digest+"/grant", `{"actor":"local-owner"}`)
	if w.Code != http.StatusAccepted {
		t.Fatalf("grant=%d %s", w.Code, w.Body.String())
	}
	before := len(s.Events("side-run", 0))
	w = call(t, h, "POST", "/api/v1/runs/side-run/approvals/"+digest+"/grant", `{"actor":"local-owner"}`)
	if w.Code != http.StatusOK || len(s.Events("side-run", 0)) != before {
		t.Fatalf("duplicate grant not idempotent: %d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/side-run/tasks/publish/result", "")
	if w.Code != http.StatusOK {
		t.Fatalf("result=%d %s", w.Code, w.Body.String())
	}
	seenCommand, seenCompleted := false, false
	for _, event := range s.Events("side-run", 0) {
		seenCommand = seenCommand || event.Type == "command.requested"
		seenCompleted = seenCompleted || event.Type == "dispatch.completed"
	}
	if !seenCommand || !seenCompleted {
		t.Fatal("approved side effect was not executed to an honest terminal")
	}
}

func TestEmergencyStopIsRecoveredFailClosed(t *testing.T) {
	db := filepath.Join(t.TempDir(), "d.db")
	s, _ := core.OpenStore(db)
	h := New("dispatch", s)
	seedDefinitions(t, h)
	if w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"stopped","catalog_id":"cat","dag_id":"dag"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/stopped/controls/emergency-stop", `{"reason":"operator"}`); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	_ = s.Close()
	s, _ = core.OpenStore(db)
	defer s.Close()
	h = New("dispatch", s)
	w := call(t, h, "POST", "/api/v1/runs/stopped/controls/resume", `{"task_id":"task-1","idempotency_key":"resume-after-restart","fencing_token":1}`)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "emergency stop") {
		t.Fatalf("control=%d %s", w.Code, w.Body.String())
	}
}

func TestApprovalContinuationCompletesRemainingDAG(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	cat := `{"id":"chain-cat","catalog":{"Version":"v1","Capabilities":[{"Name":"read","Version":"v1","SideEffecting":false},{"Name":"write","Version":"v1","SideEffecting":true}],"Agents":[{"ID":"worker","Version":"v1","Capabilities":["read@v1","write@v1"],"Priority":1,"Enabled":true,"MaxConcurrent":3}]}}`
	if w := call(t, h, "POST", "/api/v1/agents", cat); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	dag := `{"id":"chain-dag","dag":{"Version":"v1","Tasks":[{"ID":"first","Version":"v1","Requires":["read@v1"],"Cost":1},{"ID":"middle","Version":"v1","Requires":["write@v1"],"DependsOn":["first"],"Cost":1},{"ID":"last","Version":"v1","Requires":["read@v1"],"DependsOn":["middle"],"Cost":1}],"MaxFanout":2,"Budget":3}}`
	if w := call(t, h, "POST", "/api/v1/tasks", dag); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"chain","catalog_id":"chain-cat","dag_id":"chain-dag"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/tasks/first/result", ""); w.Code != 200 {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	var digest string
	for _, e := range s.Events("chain", 0) {
		if e.Type == "approval.requested" {
			digest = stringValue(e.Data["action_digest"])
		}
	}
	if digest == "" {
		t.Fatal("middle approval not requested")
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/approvals/"+digest+"/grant", `{"actor":"local-owner"}`); w.Code != 202 {
		t.Fatalf("grant=%d %s", w.Code, w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/tasks/middle/result", ""); w.Code != 200 {
		t.Fatalf("middle=%d %s", w.Code, w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/tasks/last/result", ""); w.Code != 200 {
		t.Fatalf("last=%d %s", w.Code, w.Body.String())
	}
	p, _ := s.Replay("chain", 0)
	if p.Status != "completed" {
		t.Fatalf("status=%s", p.Status)
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
	w = call(t, h, "POST", "/api/v1/runs/unknown/reconcile", `{"idempotency_key":"unknown:task-1:dispatch","result":"observed-not-applied","evidence":"operator checked upstream request log entry 42"}`)
	if w.Code != 202 {
		t.Fatalf("reconcile: %d %s", w.Code, w.Body.String())
	}
}

func TestOpenClawRunUsesHTTPAdapterAndDoesNotPersistToken(t *testing.T) {
	const token = "openclaw-test-secret"
	var hits int
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/api/v1/dispatch/dispatch" {
			t.Fatalf("unexpected gateway path %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Fatalf("authorization=%q", got)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request["task_id"] != "task-1" {
			t.Fatalf("request=%v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"task_id":"task-1","accepted":true}`))
	}))
	defer gateway.Close()
	db := filepath.Join(t.TempDir(), "dispatch.db")
	s, err := core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, err := NewConfigured("dispatch", s, "", gateway.URL, token)
	if err != nil {
		t.Fatal(err)
	}
	seedDefinitions(t, h)
	body := `{"id":"openclaw","catalog_id":"cat","dag_id":"dag","adapter":"openclaw","fixture_mode":"not-a-mode"}`
	w := call(t, h, "POST", "/api/v1/dispatch/run", body)
	if w.Code != http.StatusCreated {
		t.Fatalf("run: %d %s", w.Code, w.Body.String())
	}
	if hits != 1 {
		t.Fatalf("OpenClaw gateway hits=%d; fixture may have been used", hits)
	}
	events, _ := json.Marshal(s.Events("openclaw", 0))
	if !strings.Contains(string(events), "task.delegated") || strings.Contains(string(events), token) {
		t.Fatalf("events=%s", events)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(db + "*")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), token) {
			t.Fatalf("OpenClaw token was persisted in %s", filepath.Base(name))
		}
	}
}

func TestRunCannotSelectOpenClawCredentialOrEndpoint(t *testing.T) {
	s, err := core.OpenStore(filepath.Join(t.TempDir(), "dispatch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	for _, extra := range []string{`,"token_env":"HOME"`, `,"endpoint":"https://attacker.example"`} {
		body := `{"id":"blocked","catalog_id":"cat","dag_id":"dag","adapter":"openclaw"` + extra + `}`
		w := call(t, h, "POST", "/api/v1/dispatch/run", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("request-controlled OpenClaw configuration accepted: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestUnknownAdapterFailsClosedBeforeCreatingRun(t *testing.T) {
	s, err := core.OpenStore(filepath.Join(t.TempDir(), "dispatch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"bad","catalog_id":"cat","dag_id":"dag","adapter":"surprise"}`)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if _, ok := s.GetRun("bad"); ok {
		t.Fatal("unknown adapter created a run")
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
