package core

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEventJSONMatchesCanonicalEnvelope(t *testing.T) {
	s := NewStore()
	_ = s.CreateRun(Run{ID: "r", Mode: "dispatch"})
	e, _ := s.Append("r", "run.started", map[string]any{"x": 1})
	raw, _ := json.Marshal(e)
	var v map[string]any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema_version", "event_id", "run_id", "sequence", "occurred_at", "recorded_at", "source", "type", "payload", "redaction"} {
		if _, ok := v[key]; !ok {
			t.Fatalf("missing %s in %s", key, raw)
		}
	}
	for _, legacy := range []string{"id", "at", "data"} {
		if _, ok := v[legacy]; ok {
			t.Fatalf("legacy field %s in %s", legacy, raw)
		}
	}
}

func TestRedactionBeforeStorage(t *testing.T) {
	s := NewStore()
	if err := s.CreateRun(Run{ID: "r", Mode: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	_, err := s.Append("r", "tool.called", map[string]any{
		"authorization": "Bearer top-secret",
		"message":       "use Bearer abc.def",
		"nested":        map[string]any{"api_key": "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	b := s.Events("r", 0)[0]
	if b.Data["authorization"] != "[REDACTED]" {
		t.Fatalf("key was not redacted: %#v", b.Data)
	}
	if strings.Contains(b.Data["message"].(string), "abc.def") {
		t.Fatal("bearer token was not redacted")
	}
	if b.Data["nested"].(map[string]any)["api_key"] != "[REDACTED]" {
		t.Fatal("nested secret was not redacted")
	}
}

func TestReplayAtEvent(t *testing.T) {
	s := NewStore()
	_ = s.CreateRun(Run{ID: "r", Mode: "dispatch"})
	e1, _ := s.Append("r", "dispatch.started", nil)
	_, _ = s.Append("r", "dispatch.completed", nil)
	p, err := s.Replay("r", e1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "running" || p.Steps != 1 {
		t.Fatalf("unexpected projection: %#v", p)
	}
	final, _ := s.Replay("r", 0)
	if final.Status != "completed" || final.Steps != 2 {
		t.Fatalf("unexpected final: %#v", final)
	}
}

func TestFixturesDeterministicAndIndependent(t *testing.T) {
	for _, mode := range []string{"dispatch"} {
		s := NewStore()
		r, err := LoadFixture(s, mode)
		if err != nil {
			t.Fatal(err)
		}
		again, err := LoadFixture(s, mode)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != again.ID || len(s.ListRuns(mode)) != 1 {
			t.Fatalf("fixture not idempotent for %s", mode)
		}
		p, _ := s.Replay(r.ID, 0)
		if p.Status != "interrupted" {
			t.Fatalf("fixture must honestly expose unresolved outcome: %#v", p)
		}
	}
}
