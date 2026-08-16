package core

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistentStoreReopensAndContinuesOrderedIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatch.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(Run{ID: "run-1", Mode: "dispatch", Name: "reopen"}); err != nil {
		t.Fatal(err)
	}
	e1, err := s.Append("run-1", "dispatch.started", map[string]any{"case": "one"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.Replay("run-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != "running" || p.Steps != 1 || p.LastEventID != e1.ID {
		t.Fatalf("bad recovered projection: %#v", p)
	}
	e2, err := s.Append("run-1", "dispatch.completed", map[string]any{"score": 1})
	if err != nil {
		t.Fatal(err)
	}
	if e2.ID <= e1.ID {
		t.Fatalf("IDs not monotonic across restart: %d then %d", e1.ID, e2.ID)
	}
	p, _ = s.Replay("run-1", 0)
	if p.Status != "completed" || p.Steps != 2 {
		t.Fatalf("bad final projection: %#v", p)
	}
}

func TestPersistentStoreRejectsCorruptSequence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.CreateRun(Run{ID: "r", Mode: "dispatch"})
	_, _ = s.Append("r", "run.started", map[string]any{})
	_, _ = s.Append("r", "run.completed", map[string]any{})
	s.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE events SET sequence=3,event_id='r-3' WHERE sequence=2`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if reopened, err := OpenStore(path); err == nil {
		reopened.Close()
		t.Fatal("corrupt sequence accepted")
	}
}

func TestPersistentStoreRedactsBeforeSQLite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dispatch.db")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateRun(Run{ID: "run-secret", Mode: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	secret := "uniquely-sensitive-credential"
	if _, err := s.Append("run-secret", "tool.called", map[string]any{"api_key": secret, "message": "Bearer " + secret}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	events := s.Events("run-secret", 0)
	s.Close()
	if len(events) != 1 || events[0].Data["api_key"] != "[REDACTED]" {
		t.Fatalf("unexpected persisted data: %#v", events)
	}
	for _, suffix := range []string{"", "-wal"} {
		b, err := os.ReadFile(path + suffix)
		if err != nil && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), secret) {
			t.Fatalf("secret visible in sqlite file %s", filepath.Base(path+suffix))
		}
	}
}

func TestSeparateDatabaseFilesDoNotShareModes(t *testing.T) {
	dir := t.TempDir()
	primary, err := OpenStore(filepath.Join(dir, "dispatch.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := primary.CreateRun(Run{ID: "b", Mode: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	primary.Close()
	other, err := OpenStore(filepath.Join(dir, "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, ok := other.GetRun("b"); ok {
		t.Fatal("run leaked between mode databases")
	}
}
