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
			if event.Type == "dispatch.started" {
				if ms := uint64Number(event.Data["timeout_ms"]); ms > 0 {
					s.runTimeouts[run.ID] = time.Duration(ms) * time.Millisecond
				}
			}
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
	s.runTimeouts[in.ID] = timeout
	s.mu.Unlock()
	if _, appendErr := s.appendCritical(in.ID, "dispatch.started", map[string]any{"catalog_id": in.CatalogID, "dag_id": in.DAGID, "catalog_version": catalog.Version, "dag_version": dag.Version, "adapter": defaultString(in.Adapter, "fixture"), "fixture_mode": in.FixtureMode, "timeout_ms": timeout.Milliseconds()}); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable run start failed"})
		return
	}
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
		if e.Type == "command.completed" || e.Type == "command.cancelled" || e.Type == "command.failed" || e.Type == "command.outcome_unknown" {
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
		if _, appendErr := s.appendCritical(id, "routing.decided", map[string]any{"task_id": task.ID, "decision": decision}); appendErr != nil {
			return appendErr
		}
		requires := false
		for _, capability := range task.Requires {
			requires = requires || side[capability]
		}
		action := dispatch.Action{Tool: strings.Join(task.Requires, ","), Target: task.ID, Arguments: map[string]string{"agent": decision.Selected}, InputVersions: map[string]string{"task": task.Version, "dag": dag.Version}, PolicyVersion: "capability/v1", Scope: "dispatch:" + task.ID}
		digest := dispatch.ActionDigest(action)
		if requires && !granted[digest] {
			if !requested[digest] {
				if _, err := s.appendCritical(id, "approval.requested", map[string]any{"task_id": task.ID, "action": action, "action_digest": digest, "expires": time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339), "scope": action.Scope, "authorized_actors": []string{"local-owner"}}); err != nil {
					return err
				}
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
	cmd, created, err := ctrl.RequestOnce(dispatch.Command{ID: key, IdempotencyKey: key, TaskID: task.ID}, time.Now())
	if err != nil {
		return err
	}
	if !created {
		return nil
	}
	cmd, err = ctrl.RotateFenceForCommand(key, task.ID)
	if err != nil {
		return err
	}
	fence := cmd.Fence
	if _, err = s.appendCritical(id, "command.requested", commandData(cmd, "dispatch")); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeoutFor(id))
	ack, err := adapter.Dispatch(ctx, adapters.DispatchRequest{TaskID: task.ID, Agent: agent, IdempotencyKey: key, FencingToken: fence})
	cancel()
	if err == nil && (ack.TaskID != task.ID || !ack.Accepted) {
		err = fmt.Errorf("dispatch rejected or returned mismatched task_id")
	}
	if err != nil {
		if adapters.IsOutcomeUnknown(err) {
			cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
			cmd, _ = ctrl.Transition(key, dispatch.CommandOutcomeUnknown, "", err.Error(), time.Now())
			if _, appendErr := s.appendCritical(id, "command.outcome_unknown", commandData(cmd, "dispatch")); appendErr != nil {
				return appendErr
			}
			if _, appendErr := s.appendCritical(id, "dispatch.interrupted", map[string]any{"outcome": "outcome_unknown", "reconciliation_required": true}); appendErr != nil {
				return appendErr
			}
			return nil
		}
		cmd, _ = ctrl.Transition(key, dispatch.CommandFailed, "", err.Error(), time.Now())
		if _, appendErr := s.appendCritical(id, "command.failed", commandData(cmd, "dispatch")); appendErr != nil {
			return appendErr
		}
		if _, appendErr := s.appendCritical(id, "dispatch.failed", map[string]any{"outcome": "failed"}); appendErr != nil {
			return appendErr
		}
		return nil
	}
	cmd, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
	if _, err = s.appendCritical(id, "command.acknowledged", commandData(cmd, "dispatch")); err != nil {
		return err
	}
	if _, err = s.appendCritical(id, "task.delegated", map[string]any{"task_id": task.ID, "agent_id": agent, "accepted": ack.Accepted, "fencing_token": fence}); err != nil {
		return err
	}
	return nil
}

func (s *Server) timeoutFor(id string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.runTimeouts[id]; d > 0 {
		return d
	}
	return 15 * time.Second
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
	if action == "reassign" && strings.TrimSpace(in.Agent) == "" {
		write(w, 422, map[string]string{"error": "reassign requires target agent"})
		return
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
	var reassignDecision dispatch.RouteDecision
	if action == "reassign" {
		var validationErr error
		reassignDecision, validationErr = s.validateReassign(id, in.TaskID, in.Agent)
		if validationErr != nil {
			write(w, 422, map[string]string{"error": validationErr.Error()})
			return
		}
	}
	if action == "retry" {
		for _, candidate := range ctrl.SnapshotCommands() {
			if candidate.TaskID == in.TaskID && candidate.State == dispatch.CommandOutcomeUnknown {
				write(w, 409, map[string]string{"error": "retry forbidden while outcome is unknown; reconcile first"})
				return
			}
		}
	}
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
	cmd, created, err := ctrl.RequestOnce(dispatch.Command{ID: in.IdempotencyKey, IdempotencyKey: in.IdempotencyKey, TaskID: defaultString(in.TaskID, "run"), Fence: in.FencingToken}, time.Now())
	if err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	if !created {
		write(w, http.StatusOK, cmd)
		return
	}
	if action == "resume" || action == "retry" || action == "reassign" {
		cmd, err = ctrl.RotateFenceForCommand(in.IdempotencyKey, in.TaskID)
		if err != nil {
			write(w, 409, map[string]string{"error": err.Error()})
			return
		}
		in.FencingToken = cmd.Fence
	}
	if _, err = s.appendCritical(id, "control.requested", commandData(cmd, action)); err != nil {
		write(w, 500, map[string]string{"error": "durable control request failed"})
		return
	}
	if action == "emergency-stop" {
		ctrl.EmergencyStop(in.Reason)
		cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "", "", time.Now())
		if _, appendErr := s.appendCritical(id, "control.acknowledged", commandData(cmd, action)); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
		if _, appendErr := s.appendCritical(id, "dispatch.emergency_stopped", map[string]any{"reason": in.Reason}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
		write(w, 202, cmd)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeoutFor(id))
	defer cancel()
	var ack adapters.Acknowledgement
	switch action {
	case "pause":
		ack, err = adapter.Pause(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "cancel":
		ack, err = adapter.Cancel(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "resume":
		ack, err = adapter.Resume(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "retry":
		ack, err = adapter.Retry(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken})
	case "reassign":
		ack, err = adapter.Reassign(ctx, adapters.ControlRequest{TaskID: in.TaskID, IdempotencyKey: in.IdempotencyKey, FencingToken: in.FencingToken, Agent: in.Agent})
	}
	if err != nil {
		if adapters.IsOutcomeUnknown(err) {
			cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "", "", time.Now())
			cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandOutcomeUnknown, "", err.Error(), time.Now())
			if _, appendErr := s.appendCritical(id, "control.outcome_unknown", commandData(cmd, action)); appendErr != nil {
				write(w, 500, map[string]string{"error": "durable terminal append failed"})
				return
			}
			write(w, 202, cmd)
			return
		}
		cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandFailed, "", err.Error(), time.Now())
		if _, appendErr := s.appendCritical(id, "control.failed", commandData(cmd, action)); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
		write(w, 502, cmd)
		return
	}
	if ack.TaskID != in.TaskID || !ack.Accepted {
		cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandFailed, "", "control rejected or returned mismatched task_id", time.Now())
		if _, appendErr := s.appendCritical(id, "control.failed", commandData(cmd, action)); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
		write(w, 502, cmd)
		return
	}
	cmd, _ = ctrl.Transition(in.IdempotencyKey, dispatch.CommandAcknowledged, "accepted", "", time.Now())
	if _, appendErr := s.appendCritical(id, "control.acknowledged", commandData(cmd, action)); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable terminal append failed"})
		return
	}
	if _, appendErr := s.appendCritical(id, eventName, map[string]any{"task_id": in.TaskID, "agent_id": in.Agent, "accepted": ack.Accepted, "fencing_token": in.FencingToken}); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable terminal append failed"})
		return
	}
	if action == "reassign" {
		if _, appendErr := s.appendCritical(id, "routing.decided", map[string]any{"task_id": in.TaskID, "decision": reassignDecision, "reason": "operator_reassign"}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable routing evidence failed"})
			return
		}
	}
	if action == "retry" {
		_, _ = s.Store.Append(id, "dispatch.resumed", map[string]any{"reason": "safe_retry", "task_id": in.TaskID})
		retryKey := id + ":" + in.TaskID + ":dispatch:retry:" + in.IdempotencyKey
		retryCmd, created, retryErr := ctrl.RequestOnce(dispatch.Command{ID: retryKey, IdempotencyKey: retryKey, TaskID: in.TaskID, Fence: in.FencingToken}, time.Now())
		if retryErr != nil {
			write(w, 409, map[string]string{"error": retryErr.Error()})
			return
		}
		if created {
			_, _ = s.Store.Append(id, "command.requested", commandData(retryCmd, "dispatch_retry"))
			retryCmd, _ = ctrl.Transition(retryKey, dispatch.CommandAcknowledged, "", "", time.Now())
			_, _ = s.Store.Append(id, "command.acknowledged", commandData(retryCmd, "dispatch_retry"))
		}
	}
	write(w, 202, cmd)
}

func (s *Server) validateReassign(id, taskID, target string) (dispatch.RouteDecision, error) {
	catalog, dag, ok := s.runDefinitions(id)
	if !ok {
		return dispatch.RouteDecision{}, errors.New("run definitions unavailable")
	}
	var task dispatch.Task
	found := false
	for _, candidate := range dag.Tasks {
		if candidate.ID == taskID {
			task = candidate
			found = true
			break
		}
	}
	if !found {
		return dispatch.RouteDecision{}, errors.New("task not found in run DAG")
	}
	active := map[string]int{}
	assigned := map[string]string{}
	terminal := map[string]bool{}
	for _, event := range s.Store.Events(id, 0) {
		tid := stringValue(event.Data["task_id"])
		if event.Type == "task.delegated" || event.Type == "task.reassigned" {
			assigned[tid] = stringValue(event.Data["agent_id"])
			terminal[tid] = false
		}
		if event.Type == "task.completed" || event.Type == "task.failed" || event.Type == "task.cancelled" {
			terminal[tid] = true
		}
	}
	for tid, agent := range assigned {
		if tid != taskID && !terminal[tid] && agent != "" {
			active[agent]++
		}
	}
	states := map[string]dispatch.AgentState{}
	for agent, count := range active {
		states[agent] = dispatch.AgentState{Active: count}
	}
	decision, _ := dispatch.Route(task, catalog.Agents, states, "operator-reassign/v1")
	for _, candidate := range decision.Candidates {
		if candidate.AgentID == target {
			if !candidate.Eligible {
				return decision, fmt.Errorf("target agent %q is ineligible: %s", target, strings.Join(candidate.Reasons, ","))
			}
			decision.Selected = target
			return decision, nil
		}
	}
	return decision, fmt.Errorf("target agent %q is not in the run catalog", target)
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
		if event.Type == "approval.denied" && stringValue(event.Data["action_digest"]) == digest {
			write(w, http.StatusOK, map[string]any{"accepted": true, "action_digest": digest, "decision": "deny", "idempotent": true})
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
	claim := id + ":" + digest
	s.mu.Lock()
	if s.approvalClaims[claim] {
		s.mu.Unlock()
		write(w, http.StatusConflict, map[string]any{"error": "approval decision is already in progress", "action_digest": digest})
		return
	}
	s.approvalClaims[claim] = true
	s.mu.Unlock()
	_, err = s.appendCritical(id, "approval."+map[string]string{"grant": "granted", "deny": "denied"}[decision], map[string]any{"action_digest": digest, "actor": in.Actor, "scope": scope, "request_event_id": requested.ID})
	if err != nil {
		s.mu.Lock()
		delete(s.approvalClaims, claim)
		s.mu.Unlock()
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if decision == "deny" {
		if _, appendErr := s.appendCritical(id, "dispatch.failed", map[string]any{"outcome": "approval_denied", "task_id": taskID, "action_digest": digest}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
	} else if taskID != "" {
		_, _ = s.Store.Append(id, "dispatch.resumed", map[string]any{"reason": "approval_granted", "task_id": taskID})
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
	for _, candidate := range ctrl.SnapshotCommands() {
		if candidate.TaskID == taskID && candidate.State == dispatch.CommandAcknowledged && (!ok || candidate.UpdatedAt.After(cmd.UpdatedAt)) {
			cmd, ok = candidate, true
			key = candidate.IdempotencyKey
		}
	}
	if !ok || cmd.State != dispatch.CommandAcknowledged {
		write(w, 409, map[string]string{"error": "task is not awaiting a result"})
		return
	}
	cmd, err := ctrl.Transition(key, dispatch.CommandPolling, "", "", time.Now())
	if err != nil {
		write(w, 409, map[string]string{"error": "result check already in progress"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeoutFor(id))
	result, err := adapter.Result(ctx, adapters.TaskRef{TaskID: taskID})
	cancel()
	if err != nil {
		_, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
		write(w, 502, map[string]string{"error": err.Error()})
		return
	}
	if result.TaskID != taskID {
		_, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
		write(w, 502, map[string]string{"error": "result returned mismatched task_id"})
		return
	}
	if result.State != "completed" && result.State != "failed" && result.State != "cancelled" {
		_, _ = ctrl.Transition(key, dispatch.CommandAcknowledged, "", "", time.Now())
		write(w, 202, result)
		return
	}
	if result.State == "failed" {
		cmd, _ = ctrl.Transition(key, dispatch.CommandFailed, "", result.State, time.Now())
		for _, event := range []struct {
			typ  string
			data map[string]any
		}{{"command.failed", commandData(cmd, "dispatch")}, {"task.failed", map[string]any{"task_id": taskID, "result": result}}, {"dispatch.failed", map[string]any{"outcome": "failed"}}} {
			if _, appendErr := s.appendCritical(id, event.typ, event.data); appendErr != nil {
				write(w, 500, map[string]string{"error": "durable terminal append failed"})
				return
			}
		}
		write(w, 200, result)
		return
	}
	if result.State == "cancelled" {
		cmd, _ = ctrl.Transition(key, dispatch.CommandCancelled, "cancelled", "", time.Now())
		for _, event := range []struct {
			typ  string
			data map[string]any
		}{{"command.cancelled", commandData(cmd, "dispatch")}, {"task.cancelled", map[string]any{"task_id": taskID, "result": result}}, {"dispatch.cancelled", map[string]any{"outcome": "cancelled"}}} {
			if _, appendErr := s.appendCritical(id, event.typ, event.data); appendErr != nil {
				write(w, 500, map[string]string{"error": "durable terminal append failed"})
				return
			}
		}
		write(w, 200, result)
		return
	}
	cmd, _ = ctrl.Transition(key, dispatch.CommandCompleted, result.State, "", time.Now())
	if _, appendErr := s.appendCritical(id, "command.completed", commandData(cmd, "dispatch")); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable terminal append failed"})
		return
	}
	if _, appendErr := s.appendCritical(id, "task.completed", map[string]any{"task_id": taskID, "result": result}); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable terminal append failed"})
		return
	}
	catalog, dag, ok := s.runDefinitions(id)
	if ok {
		if scheduleErr := s.scheduleRun(id, catalog, dag); scheduleErr != nil {
			write(w, 500, map[string]string{"error": scheduleErr.Error()})
			return
		}
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
		if _, appendErr := s.appendCritical(id, "dispatch.completed", map[string]any{"outcome": "observed_results"}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable terminal append failed"})
			return
		}
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
		Disposition    string `json:"disposition"`
	}
	if err := decodeBounded(w, r, &in); err != nil || in.IdempotencyKey == "" || strings.TrimSpace(in.Result) == "" || strings.TrimSpace(in.Evidence) == "" || (in.Disposition != "applied" && in.Disposition != "not_applied" && in.Disposition != "still_unknown") {
		write(w, 400, map[string]string{"error": "idempotency_key, result, evidence and disposition (applied|not_applied|still_unknown) required"})
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
	req, err := s.appendCritical(id, "reconciliation.requested", map[string]any{"idempotency_key": in.IdempotencyKey, "command_id": cmd.ID, "evidence": in.Evidence, "disposition": in.Disposition})
	if err != nil {
		write(w, 500, map[string]string{"error": err.Error()})
		return
	}
	if in.Disposition == "still_unknown" {
		if _, appendErr := s.appendCritical(id, "reconciliation.still_unknown", map[string]any{"idempotency_key": in.IdempotencyKey, "command_id": cmd.ID, "result": in.Result, "evidence": in.Evidence, "request_event_id": req.ID}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable reconciliation append failed"})
			return
		}
		write(w, 202, map[string]any{"reconciled": false, "disposition": in.Disposition, "command": cmd})
		return
	}
	cmd, err = ctrl.Reconcile(in.IdempotencyKey, in.Result, time.Now())
	if err != nil {
		write(w, 409, map[string]string{"error": err.Error()})
		return
	}
	if _, appendErr := s.appendCritical(id, "reconciliation.completed", map[string]any{"idempotency_key": in.IdempotencyKey, "command_id": cmd.ID, "result": in.Result, "evidence": in.Evidence, "disposition": in.Disposition, "request_event_id": req.ID}); appendErr != nil {
		write(w, 500, map[string]string{"error": "durable reconciliation append failed"})
		return
	}
	if in.Disposition == "applied" {
		if _, appendErr := s.appendCritical(id, "task.completed", map[string]any{"task_id": cmd.TaskID, "reconciled": true, "result": in.Result}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable task append failed"})
			return
		}
		if catalog, dag, ok := s.runDefinitions(id); ok {
			_ = s.scheduleRun(id, catalog, dag)
		}
	} else {
		if _, appendErr := s.appendCritical(id, "task.retry_allowed", map[string]any{"task_id": cmd.TaskID, "reason": "reconciled_not_applied", "fencing_token": cmd.Fence, "evidence_event_id": req.ID}); appendErr != nil {
			write(w, 500, map[string]string{"error": "durable retry evidence failed"})
			return
		}
	}
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
		} else if in.Disposition == "applied" && s.allTasksCompleted(id) {
			_, _ = s.Store.Append(id, "dispatch.completed", map[string]any{"outcome": "reconciled_applied"})
		}
	}
	write(w, 202, map[string]any{"reconciled": true, "command": cmd})
}

func (s *Server) allTasksCompleted(id string) bool {
	_, dag, ok := s.runDefinitions(id)
	if !ok {
		return false
	}
	done := map[string]bool{}
	for _, e := range s.Store.Events(id, 0) {
		if e.Type == "task.completed" {
			done[stringValue(e.Data["task_id"])] = true
		}
	}
	for _, task := range dag.Tasks {
		if !done[task.ID] {
			return false
		}
	}
	return true
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
