package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/persistence"
)

type Event struct {
	ID       int64
	Sequence int64
	RunID    string
	Source   string
	Type     string
	At       time.Time
	Data     map[string]any
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		SchemaVersion string            `json:"schema_version"`
		EventID       string            `json:"event_id"`
		RunID         string            `json:"run_id"`
		Sequence      int64             `json:"sequence"`
		OccurredAt    time.Time         `json:"occurred_at"`
		RecordedAt    time.Time         `json:"recorded_at"`
		Source        map[string]string `json:"source"`
		Type          string            `json:"type"`
		Subject       map[string]string `json:"subject"`
		Payload       map[string]any    `json:"payload"`
		Redaction     map[string]any    `json:"redaction"`
	}{"1.0", fmt.Sprintf("%d", e.ID), e.RunID, e.Sequence, e.At, e.At, map[string]string{"product": e.Source, "component": "prototype"}, e.Type, map[string]string{"kind": "run", "id": e.RunID}, e.Data, map[string]any{"applied": true}})
}

func (e *Event) UnmarshalJSON(raw []byte) error {
	var v struct {
		SchemaVersion string    `json:"schema_version"`
		EventID       string    `json:"event_id"`
		RunID         string    `json:"run_id"`
		Sequence      int64     `json:"sequence"`
		OccurredAt    time.Time `json:"occurred_at"`
		RecordedAt    time.Time `json:"recorded_at"`
		Source        struct {
			Product   string `json:"product"`
			Component string `json:"component"`
		} `json:"source"`
		Type    string `json:"type"`
		Subject struct {
			Kind string `json:"kind"`
			ID   string `json:"id"`
		} `json:"subject"`
		Payload   map[string]any `json:"payload"`
		Redaction struct {
			Applied bool     `json:"applied"`
			Fields  []string `json:"fields,omitempty"`
		} `json:"redaction"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return err
	}
	if v.SchemaVersion != "1.0" || v.EventID == "" || v.RunID == "" || v.Sequence < 1 || v.OccurredAt.IsZero() || !v.RecordedAt.Equal(v.OccurredAt) || v.Source.Component != "prototype" || v.Source.Product != "dispatch" || !eventTypePattern.MatchString(v.Type) || v.Subject.Kind != "run" || v.Subject.ID != v.RunID || v.Payload == nil || !v.Redaction.Applied {
		return errors.New("invalid canonical event envelope")
	}
	if _, err := fmt.Sscan(v.EventID, &e.ID); err != nil || e.ID < 1 {
		return errors.New("invalid event id")
	}
	e.RunID = v.RunID
	e.Sequence = v.Sequence
	e.At = v.OccurredAt
	e.Source = v.Source.Product
	e.Type = v.Type
	e.Data = v.Payload
	return nil
}

var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

type Run struct {
	ID        string    `json:"id"`
	Mode      string    `json:"mode"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type Projection struct {
	Run         Run      `json:"run"`
	Status      string   `json:"status"`
	Steps       int      `json:"steps"`
	LastEventID int64    `json:"last_event_id"`
	Artifacts   []string `json:"artifacts,omitempty"`
}

type Store struct {
	mu     sync.RWMutex
	runs   map[string]Run
	events map[string][]Event
	nextID int64
	notify chan struct{}
	db     *persistence.SQLite
}

func (s *Store) SaveDefinition(kind, id string, value any) error {
	if kind == "" || id == "" || len(kind) > 64 || len(id) > 128 {
		return errors.New("definition kind and id required")
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(b) > 1<<20 {
		return errors.New("definition too large")
	}
	var clean any
	if err := json.Unmarshal(b, &clean); err != nil {
		return err
	}
	b, err = json.Marshal(redactValue(clean))
	if err != nil {
		return err
	}
	if s.db == nil {
		return errors.New("durable store required for definitions")
	}
	return s.db.SaveDefinition(kind, id, b)
}

func (s *Store) Definitions(kind string) (map[string]json.RawMessage, error) {
	if s.db == nil {
		return map[string]json.RawMessage{}, nil
	}
	raw, err := s.db.Definitions(kind)
	if err != nil {
		return nil, err
	}
	out := map[string]json.RawMessage{}
	for id, b := range raw {
		out[id] = json.RawMessage(b)
	}
	return out, nil
}

func NewStore() *Store {
	return &Store{runs: map[string]Run{}, events: map[string][]Event{}, notify: make(chan struct{})}
}

// OpenStore opens a durable store. Call Close before replacing or removing its database.
func OpenStore(path string) (*Store, error) {
	db, err := persistence.Open(path)
	if err != nil {
		return nil, err
	}
	runs, events, err := db.Load()
	if err != nil {
		db.Close()
		return nil, err
	}
	s := NewStore()
	s.db = db
	for _, r := range runs {
		s.runs[r.ID] = Run{ID: r.ID, Mode: r.Mode, Name: r.Name, CreatedAt: r.CreatedAt}
	}
	expected := map[string]int64{}
	seenEventIDs := map[string]bool{}
	for _, e := range events {
		run, ok := s.runs[e.RunID]
		expected[e.RunID]++
		if !ok || e.Sequence != expected[e.RunID] || e.EventID == "" || seenEventIDs[e.EventID] || e.EventID != fmt.Sprintf("%s-%d", e.RunID, e.Sequence) || e.Source != run.Mode || !eventTypePattern.MatchString(e.Type) {
			db.Close()
			return nil, fmt.Errorf("event integrity error for run %q sequence %d", e.RunID, e.Sequence)
		}
		seenEventIDs[e.EventID] = true
		s.events[e.RunID] = append(s.events[e.RunID], Event{ID: e.ID, Sequence: e.Sequence, RunID: e.RunID, Source: e.Source, Type: e.Type, At: e.At, Data: e.Data})
		if e.ID > s.nextID {
			s.nextID = e.ID
		}
	}
	return s, nil
}

func (s *Store) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *Store) CreateRun(run Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run.ID == "" || len(run.ID) > 128 || secretValue.MatchString(run.ID) {
		return errors.New("run id required")
	}
	if len(run.Name) > 256 {
		return errors.New("run name too long")
	}
	run.Name = redactValue(run.Name).(string)
	if _, ok := s.runs[run.ID]; ok {
		return errors.New("run already exists")
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}
	if s.db != nil {
		if err := s.db.CreateRun(persistence.RunRecord{ID: run.ID, Mode: run.Mode, Name: run.Name, CreatedAt: run.CreatedAt}); err != nil {
			return err
		}
	}
	s.runs[run.ID] = run
	s.signalLocked()
	return nil
}

func (s *Store) ListRuns(mode string) []Run {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Run, 0, len(s.runs))
	for _, r := range s.runs {
		if mode == "" || r.Mode == mode {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *Store) GetRun(id string) (Run, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[id]
	return r, ok
}

func (s *Store) Append(runID, typ string, data map[string]any) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !eventTypePattern.MatchString(typ) {
		return Event{}, errors.New("invalid event type")
	}
	if _, ok := s.runs[runID]; !ok {
		return Event{}, errors.New("run not found")
	}
	at := time.Now().UTC()
	clean := Redact(data)
	var e Event
	if s.db != nil {
		status := nextStatus("created", typ)
		if existing := s.events[runID]; len(existing) > 0 {
			status = nextStatus(projectStatus(existing), typ)
		}
		row, err := s.db.Append(runID, s.runs[runID].Mode, typ, at, clean, status)
		if err != nil {
			return Event{}, err
		}
		e = Event{ID: row.ID, Sequence: row.Sequence, RunID: row.RunID, Source: row.Source, Type: row.Type, At: row.At, Data: row.Data}
		s.nextID = row.ID
	} else {
		s.nextID++
		e = Event{ID: s.nextID, Sequence: int64(len(s.events[runID]) + 1), RunID: runID, Source: s.runs[runID].Mode, Type: typ, At: at, Data: clean}
	}
	s.events[runID] = append(s.events[runID], e)
	s.signalLocked()
	return e, nil
}

func nextStatus(current, typ string) string {
	switch {
	case strings.HasSuffix(typ, ".started"):
		return "running"
	case strings.HasSuffix(typ, ".completed"):
		return "completed"
	case strings.HasSuffix(typ, ".failed"):
		return "failed"
	case strings.HasSuffix(typ, ".cancelled"):
		return "cancelled"
	case strings.HasSuffix(typ, ".interrupted"):
		return "interrupted"
	case strings.HasSuffix(typ, ".emergency_stopped"):
		return "stopped"
	default:
		return current
	}
}
func projectStatus(events []Event) string {
	s := "created"
	for _, e := range events {
		s = nextStatus(s, e.Type)
	}
	return s
}

func (s *Store) Events(runID string, after int64) []Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []Event
	for _, e := range s.events[runID] {
		if e.ID > after {
			out = append(out, e)
		}
	}
	return out
}

func (s *Store) EventsLimit(runID string, after int64, limit int) ([]Event, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Event, 0, minInt(limit, len(s.events[runID])))
	for _, e := range s.events[runID] {
		if e.ID <= after {
			continue
		}
		if len(out) == limit {
			return out, true
		}
		out = append(out, e)
	}
	return out, false
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *Store) Subscribe() (<-chan struct{}, func()) {
	s.mu.RLock()
	ch := s.notify
	s.mu.RUnlock()
	return ch, func() {}
}

func (s *Store) signalLocked() { close(s.notify); s.notify = make(chan struct{}) }

func (s *Store) Replay(runID string, upto int64) (Projection, error) {
	r, ok := s.GetRun(runID)
	if !ok {
		return Projection{}, errors.New("run not found")
	}
	p := Projection{Run: r, Status: "created"}
	for _, e := range s.Events(runID, 0) {
		if upto > 0 && e.ID > upto {
			break
		}
		p.Steps++
		p.LastEventID = e.ID
		switch {
		case strings.HasSuffix(e.Type, ".started"):
			p.Status = "running"
		case strings.HasSuffix(e.Type, ".completed"):
			p.Status = "completed"
		case strings.HasSuffix(e.Type, ".failed"):
			p.Status = "failed"
		case strings.HasSuffix(e.Type, ".cancelled"):
			p.Status = "cancelled"
		case strings.HasSuffix(e.Type, ".interrupted"):
			p.Status = "interrupted"
		case strings.HasSuffix(e.Type, ".emergency_stopped"):
			p.Status = "stopped"
		case e.Type == "artifact.created":
			if v, ok := e.Data["name"].(string); ok {
				p.Artifacts = append(p.Artifacts, v)
			}
		}
	}
	return p, nil
}

var sensitiveKey = regexp.MustCompile(`(?i)(authorization|token|secret|password|api[_-]?key|cookie)`)
var bearer = regexp.MustCompile(`(?i)bearer\s+[a-z0-9._~+/=-]+`)
var secretValue = regexp.MustCompile(`(?i)(sk-[a-z0-9_-]{6,}|sentinel[-_][a-z0-9_-]+)`)

func Redact(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if sensitiveKey.MatchString(k) {
			out[k] = "[REDACTED]"
			continue
		}
		out[k] = redactValue(v)
	}
	return out
}
func redactValue(v any) any {
	switch x := v.(type) {
	case string:
		return secretValue.ReplaceAllString(bearer.ReplaceAllString(x, "Bearer [REDACTED]"), "[REDACTED]")
	case map[string]any:
		return Redact(x)
	case []any:
		a := make([]any, len(x))
		for i, v := range x {
			a[i] = redactValue(v)
		}
		return a
	default:
		return v
	}
}

func CloneJSON(v any, into any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, into)
}
func FixtureID(mode string) string { return fmt.Sprintf("%s-fixture", mode) }
