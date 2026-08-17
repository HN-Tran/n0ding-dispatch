package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
)

func TestPrivateVerticalLifecycleGate(t *testing.T) {
	var mu sync.Mutex
	dispatched := map[string]int{}
	effects := map[string]int{}
	totalCalls := 0
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			TaskID         string `json:"task_id"`
			IdempotencyKey string `json:"idempotency_key"`
			FencingToken   uint64 `json:"fencing_token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad request", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		totalCalls++
		mu.Unlock()
		switch filepath.Base(r.URL.Path) {
		case "dispatch":
			mu.Lock()
			dispatched[in.TaskID]++
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"task_id": in.TaskID, "accepted": true})
		case "result":
			_ = json.NewEncoder(w).Encode(map[string]any{"task_id": in.TaskID, "state": "completed", "output": map[string]any{"fixture": "http", "task": in.TaskID}})
		default:
			mu.Lock()
			if effects[in.IdempotencyKey] == 0 {
				effects[in.IdempotencyKey] = 1
			}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"task_id": in.TaskID, "accepted": true})
		}
	}))
	defer worker.Close()

	db := filepath.Join(t.TempDir(), "vertical.db")
	s, err := core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewConfigured("dispatch", s, "", worker.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	cat := `{"id":"vertical-cat","catalog":{"Version":"v1","Capabilities":[{"Name":"work","Version":"v1","SideEffecting":false}],"Agents":[{"ID":"worker","Version":"v1","Capabilities":["work@v1"],"Priority":1,"Enabled":true,"MaxConcurrent":1}]}}`
	dag := `{"id":"vertical-dag","dag":{"Version":"v1","Tasks":[{"ID":"first","Version":"v1","Requires":["work@v1"],"Cost":1},{"ID":"second","Version":"v1","Requires":["work@v1"],"DependsOn":["first"],"Cost":1}],"MaxFanout":1,"Budget":2}}`
	if w := call(t, h, "POST", "/api/v1/agents", cat); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/tasks", dag); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"vertical","catalog_id":"vertical-cat","dag_id":"vertical-dag","adapter":"openclaw"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	mu.Lock()
	firstDispatches := dispatched["first"]
	secondBefore := dispatched["second"]
	mu.Unlock()
	if firstDispatches != 1 || secondBefore != 0 {
		t.Fatalf("dependency released early: %+v", dispatched)
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical/controls/reassign", `{"task_id":"first","idempotency_key":"vertical-control","fencing_token":1,"agent":"worker"}`); w.Code != 202 {
		t.Fatalf("safe control=%d %s", w.Code, w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical/controls/reassign", `{"task_id":"first","idempotency_key":"vertical-control","fencing_token":1,"agent":"worker"}`); w.Code != 200 {
		t.Fatalf("duplicate control=%d %s", w.Code, w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical/controls/reassign", `{"task_id":"first","idempotency_key":"stale-control","fencing_token":1,"agent":"worker"}`); w.Code != 409 {
		t.Fatalf("stale fence=%d %s", w.Code, w.Body.String())
	}
	mu.Lock()
	controlEffects := effects["vertical-control"]
	mu.Unlock()
	if controlEffects != 1 {
		t.Fatalf("external control effects=%d", controlEffects)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	h, err = NewConfigured("dispatch", s, "", worker.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical/tasks/first/result", ""); w.Code != 200 {
		t.Fatalf("first result=%d %s", w.Code, w.Body.String())
	}
	mu.Lock()
	secondReleased := dispatched["second"]
	mu.Unlock()
	if secondReleased != 1 {
		t.Fatalf("dependent task not released: %+v", dispatched)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, err = NewConfigured("dispatch", s, "", worker.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical/tasks/second/result", ""); w.Code != 200 {
		t.Fatalf("post-restart result=%d %s", w.Code, w.Body.String())
	}
	live, err := s.Replay("vertical", 0)
	if err != nil || live.Status != "completed" {
		t.Fatalf("live=%+v err=%v", live, err)
	}
	mu.Lock()
	beforeReplayCalls := totalCalls
	mu.Unlock()
	export := call(t, h, "GET", "/api/v1/runs/vertical/export", "")
	if export.Code != 200 {
		t.Fatal(export.Body.String())
	}
	imported := call(t, h, "POST", "/api/v1/replay/import", export.Body.String())
	if imported.Code != 200 {
		t.Fatal(imported.Body.String())
	}
	var replay struct {
		Mode       string          `json:"mode"`
		Projection core.Projection `json:"projection"`
	}
	if err := json.Unmarshal(imported.Body.Bytes(), &replay); err != nil || replay.Mode != "replay" {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	mu.Lock()
	afterReplayCalls := totalCalls
	mu.Unlock()
	if afterReplayCalls != beforeReplayCalls {
		t.Fatalf("export/import invoked worker: before=%d after=%d", beforeReplayCalls, afterReplayCalls)
	}
	events := s.Events("vertical", 0)
	if live.LastEventID != events[len(events)-1].ID || replay.Projection.LastEventID != int64(len(events)) || live.Steps != replay.Projection.Steps {
		t.Fatalf("cursor/sequence mismatch live=%+v replay=%+v events=%d", live, replay.Projection, len(events))
	}
	live.LastEventID, replay.Projection.LastEventID = 0, 0
	if !bytes.Equal(mustJSON(t, live), mustJSON(t, replay.Projection)) {
		t.Fatalf("LIVE != REPLAY: live=%+v replay=%+v", live, replay.Projection)
	}

	lostWorker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		if filepath.Base(r.URL.Path) == "dispatch" {
			if hijacker, ok := w.(http.Hijacker); ok {
				conn, _, _ := hijacker.Hijack()
				_ = conn.Close()
				return
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"task_id": in["task_id"], "state": "completed"})
	}))
	defer lostWorker.Close()
	h, err = NewConfigured("dispatch", s, "", lostWorker.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	seedDefinitions(t, h)
	if w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"vertical-unknown","catalog_id":"cat","dag_id":"dag","adapter":"openclaw"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = core.OpenStore(db)
	if err != nil {
		t.Fatal(err)
	}
	h, err = NewConfigured("dispatch", s, "", lostWorker.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical-unknown/controls/retry", `{"task_id":"task-1","idempotency_key":"blocked-retry","fencing_token":1}`); w.Code != 409 {
		t.Fatalf("unknown retry=%d", w.Code)
	}
	if w := call(t, h, "POST", "/api/v1/runs/vertical-unknown/reconcile", reconciliationBody(t, s, "vertical-unknown", "vertical-unknown:task-1:dispatch", "not_applied", "hermetic fixture transcript")); w.Code != 202 {
		t.Fatalf("reconcile=%d %s", w.Code, w.Body.String())
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

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

func reconciliationBody(t *testing.T, store *core.Store, runID, key, disposition, observation string) string {
	t.Helper()
	var evidence ReconciliationEvidence
	for _, event := range store.Events(runID, 0) {
		if event.Type == "command.outcome_unknown" && stringValue(event.Data["idempotency_key"]) == key {
			evidence = ReconciliationEvidence{RunID: runID, TaskID: stringValue(event.Data["task_id"]), IdempotencyKey: key, FencingToken: uint64Number(event.Data["fence"]), CommandEventID: event.ID, Disposition: disposition, Observation: observation}
		}
	}
	if evidence.CommandEventID == 0 {
		t.Fatalf("unknown command event absent for %s", key)
	}
	evidence.Digest = ReconciliationEvidenceDigest(evidence)
	raw, err := json.Marshal(map[string]any{"idempotency_key": key, "result": observation, "disposition": disposition, "evidence": evidence})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
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
	dag := `{"id":"chain-dag","dag":{"Version":"v1","Tasks":[{"ID":"first","Version":"v1","Requires":["read@v1"],"Cost":1},{"ID":"middle","Version":"v1","Requires":["write@v1"],"DependsOn":["first"],"Cost":1},{"ID":"last","Version":"v1","Requires":["write@v1"],"DependsOn":["middle"],"Cost":1}],"MaxFanout":2,"Budget":3}}`
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
	var second string
	for _, e := range s.Events("chain", 0) {
		if e.Type == "approval.requested" {
			candidate := stringValue(e.Data["action_digest"])
			if candidate != digest {
				second = candidate
			}
		}
	}
	if second == "" {
		t.Fatal("second side-effect approval not requested")
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/approvals/"+second+"/grant", `{"actor":"local-owner"}`); w.Code != 202 {
		t.Fatalf("second grant=%d %s", w.Code, w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/chain/tasks/last/result", ""); w.Code != 200 {
		t.Fatalf("last=%d %s", w.Code, w.Body.String())
	}
	p, _ := s.Replay("chain", 0)
	if p.Status != "completed" {
		t.Fatalf("status=%s", p.Status)
	}
}

func TestPerRunTimeoutIsEffectiveAndPersisted(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	started := time.Now()
	w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"timeout","catalog_id":"cat","dag_id":"dag","fixture_mode":"timeout","timeout_ms":10}`)
	if w.Code != 201 || time.Since(started) > 500*time.Millisecond {
		t.Fatalf("status=%d elapsed=%s", w.Code, time.Since(started))
	}
	found := false
	for _, e := range s.Events("timeout", 0) {
		if e.Type == "dispatch.started" && uint64Number(e.Data["timeout_ms"]) == 10 {
			found = true
		}
	}
	if !found {
		t.Fatal("effective timeout was not persisted")
	}
}

func TestOpenClawRejectedOrMismatchedAckFailsClosed(t *testing.T) {
	for _, body := range []string{`{"task_id":"task-1","accepted":false}`, `{"task_id":"other","accepted":true}`} {
		t.Run(body, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			defer upstream.Close()
			s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
			defer s.Close()
			h, err := NewConfigured("dispatch", s, "", upstream.URL, "token")
			if err != nil {
				t.Fatal(err)
			}
			seedDefinitions(t, h)
			w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"oc","catalog_id":"cat","dag_id":"dag","adapter":"openclaw"}`)
			if w.Code != 201 {
				t.Fatalf("run=%d %s", w.Code, w.Body.String())
			}
			for _, e := range s.Events("oc", 0) {
				if e.Type == "task.delegated" {
					t.Fatal("rejected/mismatched acknowledgement delegated task")
				}
			}
			p, _ := s.Replay("oc", 0)
			if p.Status != "failed" {
				t.Fatalf("status=%s", p.Status)
			}
		})
	}
}

func TestCancelledResultDoesNotReleaseDependencies(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	cat, _ := seedDefinitions(t, h)
	dag := `{"id":"cancel-dag","dag":{"Version":"v1","Tasks":[{"ID":"task-1","Version":"v1","Requires":["research@v1"],"Cost":1},{"ID":"task-2","Version":"v1","Requires":["research@v1"],"DependsOn":["task-1"],"Cost":1}],"MaxFanout":2,"Budget":2}}`
	if w := call(t, h, "POST", "/api/v1/tasks", dag); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"cancelled","catalog_id":"`+cat+`","dag_id":"cancel-dag"}`); w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/cancelled/controls/cancel", `{"task_id":"task-1","idempotency_key":"cancel-1","fencing_token":1}`); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	if w := call(t, h, "POST", "/api/v1/runs/cancelled/tasks/task-1/result", ""); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	p, _ := s.Replay("cancelled", 0)
	if p.Status != "cancelled" {
		t.Fatalf("status=%s", p.Status)
	}
	for _, e := range s.Events("cancelled", 0) {
		if e.Type == "command.requested" && stringValue(e.Data["task_id"]) == "task-2" {
			t.Fatal("cancelled dependency released downstream task")
		}
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
	w = call(t, h, "POST", "/api/v1/runs/unknown/controls/retry", `{"task_id":"task-1","idempotency_key":"unsafe-retry","fencing_token":1}`)
	if w.Code != http.StatusConflict {
		t.Fatalf("unresolved retry=%d %s", w.Code, w.Body.String())
	}
	validEvidence := reconciliationBody(t, s, "unknown", "unknown:task-1:dispatch", "not_applied", "operator checked upstream request log entry 42")
	for name, mutate := range map[string]func(map[string]any){
		"fabricated-digest": func(e map[string]any) { e["digest"] = strings.Repeat("0", 64) },
		"wrong-task":        func(e map[string]any) { e["task_id"] = "other" },
		"wrong-run":         func(e map[string]any) { e["run_id"] = "other" },
		"wrong-fence":       func(e map[string]any) { e["fence"] = float64(99) },
		"stale-event":       func(e map[string]any) { e["command_event_id"] = float64(1) },
	} {
		var body map[string]any
		if err := json.Unmarshal([]byte(validEvidence), &body); err != nil {
			t.Fatal(err)
		}
		evidenceMap := body["evidence"].(map[string]any)
		mutate(evidenceMap)
		if name != "fabricated-digest" {
			evidenceRaw, _ := json.Marshal(evidenceMap)
			var rebound ReconciliationEvidence
			if err := json.Unmarshal(evidenceRaw, &rebound); err != nil {
				t.Fatal(err)
			}
			evidenceMap["digest"] = ReconciliationEvidenceDigest(rebound)
		}
		raw, _ := json.Marshal(body)
		if rejected := call(t, h, "POST", "/api/v1/runs/unknown/reconcile", string(raw)); rejected.Code != 403 {
			t.Fatalf("%s evidence accepted: %d %s", name, rejected.Code, rejected.Body.String())
		}
	}
	w = call(t, h, "POST", "/api/v1/runs/unknown/reconcile", validEvidence)
	if w.Code != 202 {
		t.Fatalf("reconcile: %d %s", w.Code, w.Body.String())
	}
	raw, _ = json.Marshal(s.Events("unknown", 0))
	if !strings.Contains(string(raw), "task.retry_allowed") || strings.Contains(string(raw), "task.completed") {
		t.Fatalf("not_applied semantics: %s", raw)
	}
}

func TestAppliedReconciliationCompletesObservedTask(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	_ = call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"applied","catalog_id":"cat","dag_id":"dag","fixture_mode":"lost_response"}`)
	w := call(t, h, "POST", "/api/v1/runs/applied/reconcile", reconciliationBody(t, s, "applied", "applied:task-1:dispatch", "applied", "runtime audit 42"))
	if w.Code != 202 {
		t.Fatalf("reconcile=%d %s", w.Code, w.Body.String())
	}
	p, _ := s.Replay("applied", 0)
	if p.Status != "completed" {
		t.Fatalf("status=%s", p.Status)
	}
}

func TestStillUnknownReconciliationRemainsFailClosed(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	_ = call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"still","catalog_id":"cat","dag_id":"dag","fixture_mode":"lost_response"}`)
	w := call(t, h, "POST", "/api/v1/runs/still/reconcile", reconciliationBody(t, s, "still", "still:task-1:dispatch", "still_unknown", "runtime audit incomplete"))
	if w.Code != 202 || !strings.Contains(w.Body.String(), `"reconciled":false`) {
		t.Fatalf("reconcile=%d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/still/controls/retry", `{"task_id":"task-1","idempotency_key":"blocked","fencing_token":1}`)
	if w.Code != 409 {
		t.Fatalf("retry=%d %s", w.Code, w.Body.String())
	}
}

func TestResumeRotatesFenceAndReassignRequiresAgent(t *testing.T) {
	s, _ := core.OpenStore(filepath.Join(t.TempDir(), "d.db"))
	defer s.Close()
	h := New("dispatch", s)
	seedDefinitions(t, h)
	_ = call(t, h, "POST", "/api/v1/dispatch/run", `{"id":"rotate","catalog_id":"cat","dag_id":"dag"}`)
	w := call(t, h, "POST", "/api/v1/runs/rotate/controls/reassign", `{"task_id":"task-1","idempotency_key":"bad","fencing_token":1}`)
	if w.Code != 422 {
		t.Fatalf("missing agent=%d", w.Code)
	}
	w = call(t, h, "POST", "/api/v1/runs/rotate/controls/resume", `{"task_id":"task-1","idempotency_key":"resume","fencing_token":1}`)
	if w.Code != 202 || !strings.Contains(w.Body.String(), `"fence":2`) {
		t.Fatalf("resume=%d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/rotate/controls/pause", `{"task_id":"task-1","idempotency_key":"stale","fencing_token":1}`)
	if w.Code != 409 {
		t.Fatalf("stale fence=%d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/rotate/controls/reassign", `{"task_id":"task-1","idempotency_key":"unknown-target","fencing_token":2,"agent":"not-in-catalog"}`)
	if w.Code != 422 {
		t.Fatalf("unknown target=%d %s", w.Code, w.Body.String())
	}
	w = call(t, h, "POST", "/api/v1/runs/rotate/controls/reassign", `{"task_id":"task-1","idempotency_key":"move","fencing_token":2,"agent":"scout"}`)
	if w.Code != 202 || !strings.Contains(w.Body.String(), `"fence":3`) {
		t.Fatalf("reassign=%d %s", w.Code, w.Body.String())
	}
	found := false
	for _, event := range s.Events("rotate", 0) {
		if event.Type == "routing.decided" && stringValue(event.Data["reason"]) == "operator_reassign" {
			found = true
		}
	}
	if !found {
		t.Fatal("reassign routing evidence missing")
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
