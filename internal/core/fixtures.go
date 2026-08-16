package core

import (
	"errors"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
)

func LoadFixture(s *Store, mode string) (Run, error) {
	if mode != "dispatch" {
		return Run{}, errors.New("n0ding Dispatch supports dispatch mode only")
	}
	r := Run{ID: FixtureID(mode), Mode: mode, Name: "Deterministic dispatch scenario"}
	if old, ok := s.GetRun(r.ID); ok {
		return old, nil
	}
	if err := s.CreateRun(r); err != nil {
		return Run{}, err
	}
	action := dispatch.Action{Tool: "artifact.write", Target: "finding.md", Arguments: map[string]string{"content_digest": "fixture-v1"}, PolicyVersion: "deny-default/v1", Scope: "fixture:artifact"}
	digest := dispatch.ActionDigest(action)
	approval := dispatch.Approval{Digest: digest, Actor: "fixture-owner", Scope: action.Scope, Expires: time.Now().Add(time.Hour), AuthorizedActors: []string{"fixture-owner"}}
	controller := dispatch.NewController()
	oldFence := controller.RenewLease("write-artifact")
	freshFence := controller.RenewLease("write-artifact")
	executions := 0
	_, _ = controller.ExecuteOnce("fixture-effect-1", func() (string, error) { executions++; return "artifact-created", nil })
	_, _ = controller.ExecuteOnce("fixture-effect-1", func() (string, error) { executions++; return "duplicate", nil })
	events := []struct {
		typ  string
		data map[string]any
	}{
		{"dispatch.started", map[string]any{"task": "investigate fixture", "agents": []any{"coordinator", "scout", "forge"}, "policy": "capability/v1", "dependencies": []any{"research->write-artifact", "write-artifact->external-notify"}}},
		{"task.delegated", map[string]any{"task": "research", "from": "router", "to": "scout", "reason": "research capability, stable tie-break"}},
		{"message.sent", map[string]any{"from": "scout", "to": "coordinator", "task": "research", "summary": "fixture finding ready"}},
		{"task.pause_requested", map[string]any{"task": "write-artifact", "fencing_token": oldFence}},
		{"task.paused", map[string]any{"task": "write-artifact", "acknowledged": true, "fencing_token": oldFence}},
		{"task.resumed", map[string]any{"task": "write-artifact", "fencing_token": freshFence}},
		{"approval.requested", map[string]any{"task": "write-artifact", "action": action, "action_digest": digest, "expires": approval.Expires.Format(time.RFC3339), "scope": approval.Scope, "authorized_actors": approval.AuthorizedActors}},
		{"approval.granted", map[string]any{"task": "write-artifact", "actor": approval.Actor, "action_digest": digest, "valid": approval.ValidFor(action, time.Now())}},
		{"task.failed", map[string]any{"task": "write-artifact", "attempt": 1, "error": "intentional fixture failure"}},
		{"task.retried", map[string]any{"task": "write-artifact", "attempt": 2, "retry_of": 1, "idempotency_key": "fixture-retry-1"}},
		{"artifact.created", map[string]any{"name": "finding.md", "token": "must-not-persist", "idempotency_key": "fixture-effect-1", "executions": executions}},
		{"task.outcome_unknown", map[string]any{"task": "external-notify", "retry_allowed": false, "reconciliation_required": true, "reason": "response lost after side effect"}},
		{"dispatch.interrupted", map[string]any{"outcome": "outcome_unknown", "reconciliation_required": true}},
	}
	for _, x := range events {
		if _, err := s.Append(r.ID, x.typ, x.data); err != nil {
			return Run{}, err
		}
	}
	return s.GetRunOrZero(r.ID), nil
}

func (s *Store) GetRunOrZero(id string) Run { r, _ := s.GetRun(id); return r }
