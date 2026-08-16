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

func (s *Server) recoverControllers() {
	for _, run := range s.Store.ListRuns("dispatch") {
		latest := map[string]dispatch.Command{}
		stopped, stopReason := false, ""
		for _, event := range s.Store.Events(run.ID, 0) {
			if event.Type == "dispatch.emergency_stopped" {
				stopped, stopReason = true, stringValue(event.Data["reason"])
			}
			if !strings.HasPrefix(event.Type, "command.") && !strings.HasPrefix(event.Type, "control.") {
				continue
			}
			key, _ := event.Data["idempotency_key"].(string)
			commandID, _ := event.Data["command_id"].(string)
			taskID, _ := event.Data["task_id"].(string)
			state, _ := event.Data["state"].(string)
			fence := uint64Number(event.Data["fencing_token"])
			if key == "" || commandID == "" || taskID == "" || state == "" {
				continue
			}
			latest[key] = dispatch.Command{ID: commandID, IdempotencyKey: key, TaskID: taskID, State: dispatch.CommandState(state), Fence: fence, Result: stringValue(event.Data["result"]), Error: stringValue(event.Data["error"])}
		}
		ctrl := dispatch.NewController()
		commands := make([]dispatch.Command, 0, len(latest))
		for _, cmd := range latest {
			commands = append(commands, cmd)
		}
		if ctrl.RestoreCommands(commands) == nil {
			if stopped {
				ctrl.RestoreEmergencyStop(stopReason)
			}
			s.controllers[run.ID] = ctrl
		}
	}
}

func (s *Server) recoverAdapters() {
	for _, run := range s.Store.ListRuns("dispatch") {
		for _, event := range s.Store.Events(run.ID, 0) {
			if event.Type != "dispatch.started" {
				continue
			}
			switch stringValue(event.Data["adapter"]) {
			case "openclaw":
				if s.openclaw != nil {
					s.adapters[run.ID] = s.openclaw
				}
			case "fixture", "":
				s.adapters[run.ID] = adapters.NewFixture(adapters.FixtureMode(stringValue(event.Data["fixture_mode"])))
			}
			break
		}
	}
}

func stringValue(v any) string { s, _ := v.(string); return s }
func uint64Number(v any) uint64 {
	switch n := v.(type) {
	case float64:
		return uint64(n)
	case int:
		return uint64(n)
	case int64:
		return uint64(n)
	case uint64:
		return n
	}
	return 0
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

func (s *Server) dispatchAdapter(in startRequest) (adapters.Adapter, error) {
	switch in.Adapter {
	case "", "fixture":
		return adapters.NewFixture(in.FixtureMode), nil
	case "openclaw":
		if s.openclaw == nil {
			return nil, errors.New("openclaw adapter is not configured on this server")
		}
		return s.openclaw, nil
	default:
		return nil, fmt.Errorf("unknown adapter %q", in.Adapter)
	}
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
	timeout := time.Duration(in.TimeoutMS) * time.Millisecond
	if timeout <= 0 || timeout > 5*time.Minute {
		timeout = 15 * time.Second
	}
	adapter, err := s.dispatchAdapter(in)
	if err != nil {
		write(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	if err := s.Store.CreateRun(core.Run{ID: in.ID, Mode: "dispatch", Name: in.Name}); err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	ctrl := dispatch.NewController()
	s.mu.Lock()
	s.controllers[in.ID] = ctrl
	s.adapters[in.ID] = adapter
	s.mu.Unlock()
	_, _ = s.Store.Append(in.ID, "dispatch.started", map[string]any{"catalog_id": in.CatalogID, "dag_id": in.DAGID, "catalog_version": catalog.Version, "dag_version": dag.Version, "adapter": defaultString(in.Adapter, "fixture"), "fixture_mode": in.FixtureMode})
	_ = timeout
	if err := s.scheduleRun(in.ID, catalog, dag); err != nil {
		_, _ = s.Store.Append(in.ID, "dispatch.failed", map[string]any{"error": err.Error()})
		write(w, 422, map[string]string{"error": err.Error()})
		return
	}
	run, _ := s.Store.GetRun(in.ID)
	write(w, 201, run)
}

func (s *Server) scheduleRun(id string, catalog dispatch.Catalog, dag dispatch.TaskDAG) error {
	ordered, err := topological(dag.Tasks)
	if err != nil {
		return err
	}
	events := s.Store.Events(id, 0)
	completed, active := map[string]bool{}, map[string]bool{}
	granted := map[string]bool{}
	requested := map[string]bool{}
	for _, e := range events {
		task := stringValue(e.Data["task_id"])
		if e.Type == "task.completed" {
			completed[task] = true
		}
		if e.Type == "command.acknowledged" {
			active[task] = true
		}
		if e.Type == "command.completed" || e.Type == "command.failed" || e.Type == "command.outcome_unknown" {
			active[task] = false
		}
		if e.Type == "approval.requested" {
			requested[stringValue(e.Data["action_digest"])] = true
		}
		if e.Type == "approval.granted" {
			granted[stringValue(e.Data["action_digest"])] = true
		}
	}
	side := map[string]bool{}
	for _, c := range catalog.Capabilities {
		side[c.Name+"@"+c.Version] = c.SideEffecting
	}
	for _, task := range ordered {
		if completed[task.ID] || active[task.ID] {
			continue
		}
		ready := true
		for _, dep := range task.DependsOn {
			if !completed[dep] {
				ready = false
			}
		}
		if !ready {
			continue
		}
		decision, routeErr := dispatch.Route(task, catalog.Agents, map[string]dispatch.AgentState{}, "capability/v1")
		if routeErr != nil {
			return routeErr
		}
		_, _ = s.Store.Append(id, "routing.decided", map[string]any{"task_id": task.ID, "decision": decision})
		requires := false
		for _, capability := range task.Requires {
			requires = requires || side[capability]
		}
		action := dispatch.Action{Tool: strings.Join(task.Requires, ","), Target: task.ID, Arguments: map[string]string{"agent": decision.Selected}, InputVersions: map[string]string{"task": task.Version, "dag": dag.Version}, PolicyVersion: "capability/v1", Scope: "dispatch:" + task.ID}
		digest := dispatch.ActionDigest(action)
		if requires && !granted[digest] {
			if !requested[digest] {
				_, _ = s.Store.Append(id, "approval.requested", map[string]any{"task_id": task.ID, "action": action, "action_digest": digest, "expires": time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339), "scope": action.Scope, "authorized_actors": []string{"local-owner"}})
			}
			return nil
		}
		if err := s.dispatchTask(id, task, decision.Selected); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) dispatchTask(id string, task dispatch.Task, agent string) error {
	s.mu.Lock()
	ctrl, adapter := s.controllers[id], s.adapters[id]
	s.mu.Unlock()
	if ctrl == nil || adapter == nil {
		return errors.New("run execution context unavailable")
	}
	key := id + ":" + task.ID + ":dispatch"
	if _, ok := ctrl.Command(key); ok {
		return nil
	}
	fence := ctrl.RenewLease(task.ID)
	cmd, err := ctrl.Request(dispatch.Command{ID: key, IdempotencyKey: key, TaskID: task.ID, Fence: fence}, time.Now())
	if err != nil {
		return err
	}
	_, _ = s.Store.Append(id, "command.requested", commandData(cmd, "dispatch"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	ack, err := adapter.Dispatch(ctx, adapters.DispatchRequest{TaskID: task.ID, Agent: agent, IdempotencyKey: key, FencingToken: fence})
	cancel()
	if err != nil {
		if adapters.IsOutcomeUnknown(err) {
			cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
			cmd, _ = ctrl.Transition(key, dispatch.CommandOutcomeUnknown, "", err.Error(), time.Now())
			_, _ = s.Store.Append(id, "command.outcome_unknown", commandData(cmd, "dispatch"))
			_, _ = s.Store.Append(id, "dispatch.interrupted", map[string]any{"outcome": "outcome_unknown", "reconciliation_required": true})
			return nil
		}
		cmd, _ = ctrl.Transition(key, dispatch.CommandFailed, "", err.Error(), time.Now())
		_, _ = s.Store.Append(id, "command.failed", commandData(cmd, "dispatch"))
		_, _ = s.Store.Append(id, "dispatch.failed", map[string]any{"outcome": "failed"})
		return nil
	}
	cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
	_, _ = s.Store.Append(id, "command.acknowledged", commandData(cmd, "dispatch"))
	_, _ = s.Store.Append(id, "task.delegated", map[string]any{"task_id": task.ID, "agent_id": agent, "accepted": ack.Accepted, "fencing_token": fence})
	return nil
}

func topological(tasks []dispatch.Task) ([]dispatch.Task, error) {
	byID, indegree, children := map[string]dispatch.Task{}, map[string]int{}, map[string][]string{}
	for _, task := range tasks {
		byID[task.ID] = task
		indegree[task.ID] = len(task.DependsOn)
		for _, dep := range task.DependsOn {
			children[dep] = append(children[dep], task.ID)
		}
	}
	ready := []string{}
	for id, degree := range indegree {
		if degree == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)
	out := make([]dispatch.Task, 0, len(tasks))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		out = append(out, byID[id])
		for _, child := range children[id] {
			indegree[child]--
			if indegree[child] == 0 {
				ready = append(ready, child)
				sort.Strings(ready)
			}
		}
	}
	if len(out) != len(tasks) {
		return nil, errors.New("task graph is cyclic or references an unknown dependency")
	}
	return out, nil
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
	adapter := s.adapters[id]
	if ctrl == nil {
		ctrl = dispatch.NewController()
		s.controllers[id] = ctrl
	}
	s.mu.Unlock()
	if old, ok := ctrl.Command(in.IdempotencyKey); ok {
		write(w, http.StatusOK, old)
		return
	}
	if action != "emergency-stop" {
		if in.TaskID == "" || in.FencingToken == 0 {
			write(w, 422, map[string]string{"error": "task_id and non-zero fencing_token required"})
			return
		}
		if err := ctrl.Commit(in.TaskID, in.FencingToken); err != nil {
			write(w, 409, map[string]string{"error": err.Error()})
			return
		}
	}
	if (action == "pause" || action == "cancel") && adapter == nil {
		write(w, http.StatusConflict, map[string]string{"error": "run adapter is unavailable after restart; reconcile before controlling"})
		return
	}
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
		ack, err = adapter.Pause(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "cancel":
		ack, err = adapter.Cancel(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
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
	var in struct {
		Actor string `json:"actor"`
	}
	if r.ContentLength != 0 {
		if err := decodeBounded(w, r, &in); err != nil {
			write(w, 400, map[string]string{"error": err.Error()})
			return
		}
	}
	if in.Actor == "" {
		in.Actor = "local-owner"
	}
	var requested *core.Event
	events := s.Store.Events(id, 0)
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type == "approval.requested" && stringValue(events[i].Data["action_digest"]) == digest {
			event := events[i]
			requested = &event
			break
		}
	}
	if requested == nil {
		write(w, 404, map[string]string{"error": "approval request not found"})
		return
	}
	var action dispatch.Action
	raw, err := json.Marshal(requested.Data["action"])
	if err != nil || json.Unmarshal(raw, &action) != nil {
		write(w, 422, map[string]string{"error": "invalid requested action"})
		return
	}
	expires, err := time.Parse(time.RFC3339, stringValue(requested.Data["expires"]))
	if err != nil {
		write(w, 422, map[string]string{"error": "invalid approval expiry"})
		return
	}
	scope := stringValue(requested.Data["scope"])
	if scope == "" {
		scope = action.Scope
	}
	actors := stringSlice(requested.Data["authorized_actors"])
	if len(actors) == 0 {
		actors = []string{"local-owner"}
	}
	approval := dispatch.Approval{Digest: digest, Actor: in.Actor, Scope: scope, Expires: expires, AuthorizedActors: actors}
	if !approval.ValidFor(action, time.Now()) {
		write(w, 403, map[string]string{"error": "approval is expired, mutated, out of scope, or actor is unauthorized"})
		return
	}
	taskID := stringValue(requested.Data["task_id"])
	for _, event := range events {
		if event.Type == "approval.granted" && stringValue(event.Data["action_digest"]) == digest {
			write(w, http.StatusOK, map[string]any{"accepted": true, "action_digest": digest, "decision": "grant", "idempotent": true})
			return
		}
	}
	if decision == "grant" && taskID != "" {
		s.mu.Lock()
		ctrl, adapter := s.controllers[id], s.adapters[id]
		s.mu.Unlock()
		if ctrl == nil || adapter == nil {
			write(w, 409, map[string]string{"error": "run execution context unavailable after restart; approval was not granted"})
			return
		}
	}
	_, err = s.Store.Append(id, "approval."+map[string]string{"grant": "granted", "deny": "denied"}[decision], map[string]any{"action_digest": digest, "actor": in.Actor, "scope": scope, "request_event_id": requested.ID})
	if err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if decision == "deny" {
		_, _ = s.Store.Append(id, "dispatch.failed", map[string]any{"outcome": "approval_denied", "task_id": taskID, "action_digest": digest})
	} else if taskID != "" {
		catalog, dag, ok := s.runDefinitions(id)
		if !ok {
			write(w, 409, map[string]string{"error": "run definitions unavailable"})
			return
		}
		if err := s.scheduleRun(id, catalog, dag); err != nil {
			_, _ = s.Store.Append(id, "dispatch.failed", map[string]any{"error": err.Error()})
			write(w, 422, map[string]string{"error": err.Error()})
			return
		}
	}
	write(w, 202, map[string]any{"accepted": true, "action_digest": digest, "decision": decision})
}

func (s *Server) runDefinitions(id string) (dispatch.Catalog, dispatch.TaskDAG, bool) {
	for _, event := range s.Store.Events(id, 0) {
		if event.Type == "dispatch.started" {
			s.mu.Lock()
			defer s.mu.Unlock()
			catalog, cok := s.catalogs[stringValue(event.Data["catalog_id"])]
			dag, dok := s.dags[stringValue(event.Data["dag_id"])]
			return catalog, dag, cok && dok
		}
	}
	return dispatch.Catalog{}, dispatch.TaskDAG{}, false
}

func (s *Server) taskResult(w http.ResponseWriter, r *http.Request) {
	id, taskID := r.PathValue("id"), r.PathValue("task")
	if !s.owns(id) {
		write(w, 404, map[string]string{"error": "run not found"})
		return
	}
	s.mu.Lock()
	ctrl, adapter := s.controllers[id], s.adapters[id]
	s.mu.Unlock()
	if ctrl == nil || adapter == nil {
		write(w, 409, map[string]string{"error": "run execution context unavailable"})
		return
	}
	key := id + ":" + taskID + ":dispatch"
	cmd, ok := ctrl.Command(key)
	if !ok || cmd.State != dispatch.CommandAcknowledged {
		write(w, 409, map[string]string{"error": "task is not awaiting a result"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	result, err := adapter.Result(ctx, adapters.TaskRef{TaskID: taskID})
	cancel()
	if err != nil {
		write(w, 502, map[string]string{"error": err.Error()})
		return
	}
	if result.State != "completed" && result.State != "failed" && result.State != "cancelled" {
		write(w, 202, result)
		return
	}
	if result.State == "failed" {
		cmd, _ = ctrl.Transition(key, dispatch.CommandFailed, "", result.State, time.Now())
		_, _ = s.Store.Append(id, "command.failed", commandData(cmd, "dispatch"))
		_, _ = s.Store.Append(id, "task.failed", map[string]any{"task_id": taskID, "result": result})
		_, _ = s.Store.Append(id, "dispatch.failed", map[string]any{"outcome": "failed"})
		write(w, 200, result)
		return
	}
	cmd, _ = ctrl.Transition(key, dispatch.CommandCompleted, result.State, "", time.Now())
	_, _ = s.Store.Append(id, "command.completed", commandData(cmd, "dispatch"))
	_, _ = s.Store.Append(id, "task.completed", map[string]any{"task_id": taskID, "result": result})
	catalog, dag, ok := s.runDefinitions(id)
	if ok {
		_ = s.scheduleRun(id, catalog, dag)
	}
	all := true
	for _, task := range dag.Tasks {
		found := false
		for _, event := range s.Store.Events(id, 0) {
			if event.Type == "task.completed" && stringValue(event.Data["task_id"]) == task.ID {
				found = true
			}
		}
		all = all && found
	}
	if all {
		_, _ = s.Store.Append(id, "dispatch.completed", map[string]any{"outcome": "observed_results"})
	}
	write(w, 200, result)
}

func stringSlice(v any) []string {
	var out []string
	switch values := v.(type) {
	case []string:
		return append(out, values...)
	case []any:
		for _, value := range values {
			if s, ok := value.(string); ok && s != "" {
				out = append(out, s)
			}
		}
	}
	return out
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
		Evidence       string `json:"evidence"`
	}
	if err := decodeBounded(w, r, &in); err != nil || in.IdempotencyKey == "" || strings.TrimSpace(in.Result) == "" || strings.TrimSpace(in.Evidence) == "" {
		write(w, 400, map[string]string{"error": "idempotency_key, result and new evidence required"})
		return
	}
	s.mu.Lock()
	ctrl := s.controllers[id]
	s.mu.Unlock()
	if ctrl == nil {
		write(w, 409, map[string]string{"error": "controller unavailable"})
		return
	}
	cmd, ok := ctrl.Command(in.IdempotencyKey)
	if !ok || cmd.State != dispatch.CommandOutcomeUnknown {
		write(w, 409, map[string]string{"error": "command is not awaiting reconciliation"})
		return
	}
	req, err := s.Store.Append(id, "reconciliation.requested", map[string]any{"idempotency_key": in.IdempotencyKey, "command_id": cmd.ID, "evidence": in.Evidence})
	if err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	cmd, err = ctrl.Reconcile(in.IdempotencyKey, in.Result, time.Now())
	if err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	_, _ = s.Store.Append(id, "reconciliation.completed", map[string]any{"idempotency_key": in.IdempotencyKey, "command_id": cmd.ID, "result": in.Result, "evidence": in.Evidence, "request_event_id": req.ID})
	pending, failed := false, false
	for _, candidate := range ctrl.SnapshotCommands() {
		if candidate.State == dispatch.CommandOutcomeUnknown {
			pending = true
		}
		if candidate.State == dispatch.CommandFailed {
			failed = true
		}
	}
	if !pending {
		if failed {
			_, _ = s.Store.Append(id, "dispatch.failed", map[string]any{"outcome": "reconciled_with_failures"})
		} else {
			_, _ = s.Store.Append(id, "dispatch.completed", map[string]any{"outcome": "reconciled"})
		}
	}
	write(w, 202, map[string]any{"reconciled": true, "command": cmd})
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
