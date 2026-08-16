package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type RunRecord struct {
	ID, Mode, Name string
	CreatedAt      time.Time
}

type EventRecord struct {
	ID, Sequence                 int64
	EventID, RunID, Source, Type string
	At                           time.Time
	Data                         map[string]any
}

type SQLite struct{ db *sql.DB }

func Open(path string) (*SQLite, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &SQLite{db: db}
	ctx := context.Background()
	for _, q := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA synchronous=FULL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, mode TEXT NOT NULL, name TEXT NOT NULL, created_at TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, run_id TEXT NOT NULL REFERENCES runs(id), sequence INTEGER NOT NULL, source TEXT NOT NULL, type TEXT NOT NULL, at TEXT NOT NULL, data BLOB NOT NULL, UNIQUE(run_id, sequence))`,
		`CREATE TABLE IF NOT EXISTS projections (run_id TEXT PRIMARY KEY REFERENCES runs(id), status TEXT NOT NULL, steps INTEGER NOT NULL, last_event_id INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS definitions (kind TEXT NOT NULL, id TEXT NOT NULL, data BLOB NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(kind,id))`,
		`CREATE INDEX IF NOT EXISTS events_run_id_id ON events(run_id, id)`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize sqlite: %w", err)
		}
	}
	return s, nil
}

func (s *SQLite) SaveDefinition(kind, id string, data []byte) error {
	_, err := s.db.Exec(`INSERT INTO definitions(kind,id,data,updated_at) VALUES(?,?,?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at`, kind, id, data, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *SQLite) Definitions(kind string) (map[string][]byte, error) {
	rows, err := s.db.Query(`SELECT id,data FROM definitions WHERE kind=? ORDER BY id`, kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]byte{}
	for rows.Next() {
		var id string
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		out[id] = append([]byte(nil), data...)
	}
	return out, rows.Err()
}

func (s *SQLite) Close() error { return s.db.Close() }

func (s *SQLite) Load() ([]RunRecord, []EventRecord, error) {
	rows, err := s.db.Query(`SELECT id, mode, name, created_at FROM runs ORDER BY created_at, id`)
	if err != nil {
		return nil, nil, err
	}
	var runs []RunRecord
	for rows.Next() {
		var r RunRecord
		var at string
		if err := rows.Scan(&r.ID, &r.Mode, &r.Name, &at); err != nil {
			rows.Close()
			return nil, nil, err
		}
		r.CreatedAt, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		runs = append(runs, r)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	erows, err := s.db.Query(`SELECT id, event_id, run_id, sequence, source, type, at, data FROM events ORDER BY id`)
	if err != nil {
		return nil, nil, err
	}
	defer erows.Close()
	var events []EventRecord
	for erows.Next() {
		var e EventRecord
		var at string
		var data []byte
		if err := erows.Scan(&e.ID, &e.EventID, &e.RunID, &e.Sequence, &e.Source, &e.Type, &at, &data); err != nil {
			return nil, nil, err
		}
		e.At, err = time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal(data, &e.Data); err != nil {
			return nil, nil, err
		}
		events = append(events, e)
	}
	return runs, events, erows.Err()
}

func (s *SQLite) CreateRun(r RunRecord) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT INTO runs(id,mode,name,created_at) VALUES(?,?,?,?)`, r.ID, r.Mode, r.Name, r.CreatedAt.Format(time.RFC3339Nano)); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO projections(run_id,status,steps,last_event_id) VALUES(?, 'created', 0, 0)`, r.ID); err != nil {
		return err
	}
	return tx.Commit()
}

// Append atomically persists the immutable event and its derived state cursor.
func (s *SQLite) Append(runID, source, typ string, at time.Time, data map[string]any, status string) (EventRecord, error) {
	b, err := json.Marshal(data)
	if err != nil {
		return EventRecord{}, err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return EventRecord{}, err
	}
	defer tx.Rollback()
	var sequence int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(sequence),0)+1 FROM events WHERE run_id=?`, runID).Scan(&sequence); err != nil {
		return EventRecord{}, err
	}
	eventID := fmt.Sprintf("%s-%d", runID, sequence)
	res, err := tx.Exec(`INSERT INTO events(event_id,run_id,sequence,source,type,at,data) VALUES(?,?,?,?,?,?,?)`, eventID, runID, sequence, source, typ, at.Format(time.RFC3339Nano), b)
	if err != nil {
		return EventRecord{}, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return EventRecord{}, err
	}
	if _, err = tx.Exec(`UPDATE projections SET status=?, steps=steps+1, last_event_id=? WHERE run_id=?`, status, id, runID); err != nil {
		return EventRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return EventRecord{}, err
	}
	return EventRecord{ID: id, EventID: eventID, RunID: runID, Sequence: sequence, Source: source, Type: typ, At: at, Data: data}, nil
}
