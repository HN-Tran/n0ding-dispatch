package bundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
)

const MaxBundleBytes = 8 << 20

type Manifest struct {
	Format, Mode, RunID, ProjectionDigest, EventsDigest string
	EventCount                                          int
}
type Bundle struct {
	Manifest Manifest     `json:"manifest"`
	Run      core.Run     `json:"run"`
	Events   []core.Event `json:"events"`
}

func digest(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	// Normalize concrete structs and JSON-decoded maps to one representation so
	// export and import hash the same semantic event payload.
	var normalized any
	if err := json.Unmarshal(b, &normalized); err != nil {
		return "", err
	}
	b, err = json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func projectionDigest(p core.Projection) (string, error) {
	// Storage cursors may differ after import; the normalized domain projection must not.
	p.LastEventID = 0
	return digest(p)
}

func Export(store *core.Store, runID string) ([]byte, error) {
	run, ok := store.GetRun(runID)
	if !ok {
		return nil, errors.New("run not found")
	}
	events := store.Events(runID, 0)
	projection, err := store.Replay(runID, 0)
	if err != nil {
		return nil, err
	}
	ed, _ := digest(events)
	pd, _ := projectionDigest(projection)
	b := Bundle{Manifest: Manifest{Format: "n0ding-replay/v1", Mode: run.Mode, RunID: runID, ProjectionDigest: pd, EventsDigest: ed, EventCount: len(events)}, Run: run, Events: events}
	return json.MarshalIndent(b, "", "  ")
}

func VerifyAndReplay(raw []byte) (core.Projection, error) {
	if len(raw) > MaxBundleBytes {
		return core.Projection{}, errors.New("bundle exceeds size limit")
	}
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return core.Projection{}, fmt.Errorf("invalid bundle: %w", err)
	}
	if b.Manifest.Format != "n0ding-replay/v1" || b.Manifest.EventCount != len(b.Events) {
		return core.Projection{}, errors.New("manifest mismatch")
	}
	ed, _ := digest(b.Events)
	if ed != b.Manifest.EventsDigest {
		return core.Projection{}, errors.New("event checksum mismatch")
	}
	if b.Run.ID != b.Manifest.RunID || b.Run.Mode != b.Manifest.Mode {
		return core.Projection{}, errors.New("run manifest mismatch")
	}
	seenIDs := map[int64]bool{}
	for i, e := range b.Events {
		if e.RunID != b.Manifest.RunID || e.Source != b.Manifest.Mode || e.Sequence != int64(i+1) || seenIDs[e.ID] {
			return core.Projection{}, errors.New("event integrity error: scope, sequence, or id conflict")
		}
		seenIDs[e.ID] = true
	}
	s := core.NewStore()
	if err := s.CreateRun(b.Run); err != nil {
		return core.Projection{}, err
	}
	for _, e := range b.Events {
		if e.RunID != b.Manifest.RunID {
			return core.Projection{}, errors.New("cross-run event")
		}
		if _, err := s.Append(e.RunID, e.Type, e.Data); err != nil {
			return core.Projection{}, err
		}
	}
	p, err := s.Replay(b.Manifest.RunID, 0)
	if err != nil {
		return core.Projection{}, err
	}
	pd, _ := projectionDigest(p)
	if pd != b.Manifest.ProjectionDigest {
		return core.Projection{}, errors.New("projection checksum mismatch")
	}
	return p, nil
}
