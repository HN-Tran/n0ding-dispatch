package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/adapters"
	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
)

func (s *Server) loadDefinitions() {
	for _, kind := range []string{"catalog", "dag"} {
		items, _ := s.Store.Definitions(kind)
		for id, raw := range items {
			if kind == "catalog" {
				var v dispatch.Catalog
				if json.Unmarshal(raw, &v) == nil && v.Validate() == nil {
					s.catalogs[id] = v
				}
			}
			if kind == "dag" {
				var v dispatch.TaskDAG
				if json.Unmarshal(raw, &v) == nil && v.Validate() == nil {
					s.dags[id] = v
				}
			}
		}
	}
}

func (s *Server) recoverInterrupted() {
	for _, r := range s.Store.ListRuns("dispatch") {
		p, _ := s.Store.Replay(r.ID, 0)
		if p.Status == "running" {
			_, _ = s.Store.Append(r.ID, "dispatch.interrupted", map[string]any{"reason": "process_restart", "reconciliation_required": true})
		}
	}
}

func decodeBounded(w http.ResponseWriter, r *http.Request, out any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func definitionID(prefix string, v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return prefix + "-" + hex.EncodeToString(h[:8])
}

func (s *Server) listAgents(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	write(w, 200, map[string]any{"catalogs": s.catalogs})
}
func (s *Server) putCatalog(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID      string           `json:"id"`
		Catalog dispatch.Catalog `json:"catalog"`
	}
	if decodeBounded(w, r, &in) != nil {
		write(w, 400, map[string]string{"error": "invalid catalog"})
		return
	}
	if err := in.Catalog.Validate(); err != nil {
		write(w, 422, map[string]string{"error": err.Error()})
		return
	}
	if in.ID == "" {
		in.ID = definitionID("catalog", in.Catalog)
	}
	if err := s.Store.SaveDefinition("catalog", in.ID, in.Catalog); err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	s.catalogs[in.ID] = in.Catalog
	s.mu.Unlock()
	write(w, 201, map[string]any{"id": in.ID, "catalog": in.Catalog})
}
func (s *Server) listTasks(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	write(w, 200, map[string]any{"dags": s.dags})
}
func (s *Server) putDAG(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID  string           `json:"id"`
		DAG dispatch.TaskDAG `json:"dag"`
	}
	if decodeBounded(w, r, &in) != nil {
		write(w, 400, map[string]string{"error": "invalid DAG"})
		return
	}
	if err := in.DAG.Validate(); err != nil {
		write(w, 422, map[string]string{"error": err.Error()})
		return
	}
	if in.ID == "" {
		in.ID = definitionID("dag", in.DAG)
	}
	if err := s.Store.SaveDefinition("dag", in.ID, in.DAG); err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	s.mu.Lock()
	s.dags[in.ID] = in.DAG
	s.mu.Unlock()
	write(w, 201, map[string]any{"id": in.ID, "dag": in.DAG})
}

type startRequest struct {
	ID          string               `json:"id"`
	Name        string               `json:"name"`
	CatalogID   string               `json:"catalog_id"`
	DAGID       string               `json:"dag_id"`
	Adapter     string               `json:"adapter"`
	FixtureMode adapters.FixtureMode `json:"fixture_mode"`
	TimeoutMS   int                  `json:"timeout_ms"`
}

func (s *Server) startDispatch(w http.ResponseWriter, r *http.Request) {
	var in startRequest
	if err := decodeBounded(w, r, &in); err != nil {
		write(w, 400, map[string]string{"error": err.Error()})
		return
	}
	if in.ID == "" || in.CatalogID == "" || in.DAGID == "" {
		write(w, 400, map[string]string{"error": "id, catalog_id and dag_id required"})
		return
	}
	s.mu.Lock()
	catalog, cok := s.catalogs[in.CatalogID]
	dag, dok := s.dags[in.DAGID]
	s.mu.Unlock()
	if !cok || !dok {
		write(w, 404, map[string]string{"error": "catalog or DAG not found"})
		return
	}
	if err := s.Store.CreateRun(core.Run{ID: in.ID, Mode: "dispatch", Name: in.Name}); err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	ctrl := dispatch.NewController()
	s.mu.Lock()
	s.controllers[in.ID] = ctrl
	s.mu.Unlock()
	_, _ = s.Store.Append(in.ID, "dispatch.started", map[string]any{"catalog_id": in.CatalogID, "dag_id": in.DAGID, "catalog_version": catalog.Version, "dag_version": dag.Version})
	adapter := s.adapter
	if in.Adapter == "fixture" || in.Adapter == "" {
		adapter = adapters.NewFixture(in.FixtureMode)
	}
	timeout := time.Duration(in.TimeoutMS) * time.Millisecond
	if timeout <= 0 || timeout > 5*time.Minute {
		timeout = 15 * time.Second
	}
	states := map[string]dispatch.AgentState{}
	for _, task := range topological(dag.Tasks) {
		decision, err := dispatch.Route(task, catalog.Agents, states, "capability/v1")
		data := map[string]any{"task_id": task.ID, "decision": decision}
		if err != nil {
			data["error"] = err.Error()
			_, _ = s.Store.Append(in.ID, "routing.failed", data)
			_, _ = s.Store.Append(in.ID, "dispatch.failed", map[string]any{"error": "no eligible route"})
			write(w, 422, map[string]any{"run_id": in.ID, "error": err.Error()})
			return
		}
		_, _ = s.Store.Append(in.ID, "routing.decided", data)
		fence := ctrl.RenewLease(task.ID)
		key := in.ID + ":" + task.ID + ":dispatch"
		cmd, _ := ctrl.Request(dispatch.Command{ID: key, IdempotencyKey: key, TaskID: task.ID, Fence: fence}, time.Now())
		_, _ = s.Store.Append(in.ID, "command.requested", commandData(cmd, "dispatch"))
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		ack, err := adapter.Dispatch(ctx, adapters.DispatchRequest{TaskID: task.ID, Agent: decision.Selected, IdempotencyKey: key, FencingToken: fence})
		cancel()
		if err != nil {
			if adapters.IsOutcomeUnknown(err) {
				cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
				cmd, _ = ctrl.Transition(key, dispatch.CommandOutcomeUnknown, "", err.Error(), time.Now())
				_, _ = s.Store.Append(in.ID, "command.outcome_unknown", commandData(cmd, "dispatch"))
				_, _ = s.Store.Append(in.ID, "task.outcome_unknown", map[string]any{"task_id": task.ID, "reconciliation_required": true, "retry_allowed": false})
				continue
			}
			cmd, _ = ctrl.Transition(key, dispatch.CommandFailed, "", err.Error(), time.Now())
			_, _ = s.Store.Append(in.ID, "command.failed", commandData(cmd, "dispatch"))
			_, _ = s.Store.Append(in.ID, "task.failed", map[string]any{"task_id": task.ID, "error": err.Error()})
			continue
		}
		cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
		_, _ = s.Store.Append(in.ID, "command.acknowledged", commandData(cmd, "dispatch"))
		_, _ = s.Store.Append(in.ID, "task.delegated", map[string]any{"task_id": task.ID, "agent_id": decision.Selected, "accepted": ack.Accepted, "fencing_token": fence})
		cmd, _ = ctrl.Transition(key, dispatch.CommandCompleted, "accepted", "", time.Now())
		_, _ = s.Store.Append(in.ID, "command.completed", commandData(cmd, "dispatch"))
	}
	_, _ = s.Store.Append(in.ID, "dispatch.completed", map[string]any{"outcome": "observed"})
	run, _ := s.Store.GetRun(in.ID)
	write(w, 201, run)
}

func topological(tasks []dispatch.Task) []dispatch.Task {
	out := append([]dispatch.Task(nil), tasks...)
	sort.SliceStable(out, func(i, j int) bool { return len(out[i].DependsOn) < len(out[j].DependsOn) })
	return out
}
func commandData(c dispatch.Command, action string) map[string]any {
	return map[string]any{"command_id": c.ID, "idempotency_key": c.IdempotencyKey, "task_id": c.TaskID, "state": c.State, "fencing_token": c.Fence, "action": action, "result": c.Result, "error": c.Error}
}

func (s *Server) control(w http.ResponseWriter, r *http.Request) {
	id, action := r.PathValue("id"), r.PathValue("action")
	if !s.owns(id) {
		write(w, 404, map[string]string{"error": "run not found"})
		return
	}
	allowed := map[string]bool{"pause": true, "resume": true, "cancel": true, "retry": true, "reassign": true, "emergency-stop": true}
	if !allowed[action] {
		write(w, 404, map[string]string{"error": "unknown control"})
		return
	}
	var in struct {
		TaskID         string `json:"task_id"`
		Agent          string `json:"agent"`
		IdempotencyKey string `json:"idempotency_key"`
		Reason         string `json:"reason"`
		FencingToken   uint64 `json:"fencing_token"`
	}
	if r.ContentLength != 0 {
		if err := decodeBounded(w, r, &in); err != nil {
			write(w, 400, map[string]string{"error": err.Error()})
			return
		}
	}
	if in.IdempotencyKey == "" {
		in.IdempotencyKey = fmt.Sprintf("%s:%s:%s", id, in.TaskID, action)
	}
	eventName := map[string]string{"pause": "task.paused", "resume": "task.resumed", "cancel": "task.cancelled", "retry": "task.retried", "reassign": "task.reassigned"}[action]
	s.mu.Lock()
	ctrl := s.controllers[id]
	if ctrl == nil {
		ctrl = dispatch.NewController()
		s.controllers[id] = ctrl
	}
	s.mu.Unlock()
	cmd, err := ctrl.Request(dispatch.Command{ID: in.IdempotencyKey, IdempotencyKey: in.IdempotencyKey, TaskID: defaultString(in.TaskID, "run"), Fence: in.FencingToken}, time.Now())
	if err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	if cmd.State != dispatch.CommandRequested {
		write(w, http.StatusOK, cmd)
		return
	}
	_, _ = s.Store.Append(id, "control.requested", commandData(cmd, action))
	if action == "emergency-stop" {
		ctrl.EmergencyStop(in.Reason)
		cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "", "", time.Now())
		_, _ = s.Store.Append(id, "control.acknowledged", commandData(cmd, action))
		_, _ = s.Store.Append(id, "dispatch.emergency_stopped", map[string]any{"reason": in.Reason})
		write(w, 202, cmd)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ack adapters.Acknowledgement
	switch action {
	case "pause":
		ack, err = s.adapter.Pause(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "cancel":
		ack, err = s.adapter.Cancel(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	default:
		ack = adapters.Acknowledgement{TaskID: in.TaskID, Accepted: true}
	}
	if err != nil {
		if adapters.IsOutcomeUnknown(err) {
			cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "", "", time.Now())
			cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandOutcomeUnknown, "", err.Error(), time.Now())
			_, _ = s.Store.Append(id, "control.outcome_unknown", commandData(cmd, action))
			write(w, 202, cmd)
			return
		}
		cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandFailed, "", err.Error(), time.Now())
		_, _ = s.Store.Append(id, "control.failed", commandData(cmd, action))
		write(w, 502, cmd)
		return
	}
	cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "accepted", "", time.Now())
	_, _ = s.Store.Append(id, "control.acknowledged", commandData(cmd, action))
	_, _ = s.Store.Append(id, eventName, map[string]any{"task_id": in.TaskID, "agent_id": in.Agent, "accepted": ack.Accepted})
	write(w, 202, cmd)
}

func defaultString(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
func (s *Server) approve(w http.ResponseWriter, r *http.Request) {
	id, digest, decision := r.PathValue("id"), r.PathValue("digest"), r.PathValue("decision")
	if !s.owns(id) {
		write(w, 404, map[string]string{"error": "run not found"})
		return
	}
	if len(digest) != 64 || (decision != "grant" && decision != "deny") {
		write(w, 422, map[string]string{"error": "valid action digest and decision required"})
		return
	}
	_, err := s.Store.Append(id, "approval."+map[string]string{"grant": "granted", "deny": "denied"}[decision], map[string]any{"action_digest": digest, "actor": "local-owner"})
	if err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	write(w, 202, map[string]any{"accepted": true, "action_digest": digest, "decision": decision})
}
func (s *Server) reconcile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.owns(id) {
		write(w, 404, map[string]string{"error": "run not found"})
		return
	}
	var in struct {
		IdempotencyKey string `json:"idempotency_key"`
		Result         string `json:"result"`
	}
	if err := decodeBounded(w, r, &in); err != nil || in.IdempotencyKey == "" {
		write(w, 400, map[string]string{"error": "idempotency_key required"})
		return
	}
	_, _ = s.Store.Append(id, "reconciliation.requested", map[string]any{"idempotency_key": in.IdempotencyKey})
	_, _ = s.Store.Append(id, "reconciliation.completed", map[string]any{"idempotency_key": in.IdempotencyKey, "result": in.Result})
	write(w, 202, map[string]any{"reconciled": true})
}

func (s *Server) filtered(w http.ResponseWriter, r *http.Request, types ...string) {
	if !s.owns(r.PathValue("id")) {
		write(w, 404, map[string]string{"error": "run not found"})
		return
	}
	wanted := map[string]bool{}
	for _, x := range types {
		wanted[x] = true
	}
	var out []core.Event
	for _, e := range s.Store.Events(r.PathValue("id"), 0) {
		for x := range wanted {
			if e.Type == x || strings.HasPrefix(e.Type, x+".") {
				out = append(out, e)
				break
			}
		}
	}
	write(w, 200, map[string]any{"events": out})
}
func (s *Server) decisions(w http.ResponseWriter, r *http.Request) { s.filtered(w, r, "routing") }
func (s *Server) approvals(w http.ResponseWriter, r *http.Request) { s.filtered(w, r, "approval") }
func (s *Server) artifacts(w http.ResponseWriter, r *http.Request) { s.filtered(w, r, "artifact") }
func (s *Server) messages(w http.ResponseWriter, r *http.Request)  { s.filtered(w, r, "message") }
