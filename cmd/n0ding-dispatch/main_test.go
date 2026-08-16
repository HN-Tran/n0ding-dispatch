package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCLIUsageAndInit(t *testing.T) {
	if got := run(nil); got != exitUsage {
		t.Fatalf("usage=%d", got)
	}
	db := filepath.Join(t.TempDir(), "dispatch.db")
	if got := run([]string{"init", "--db", db}); got != exitOK {
		t.Fatalf("init=%d", got)
	}
	if _, err := os.Stat(db); err != nil {
		t.Fatal(err)
	}
}

func TestControlAndReconcileSendSafetyFields(t *testing.T) {
	type captured struct {
		path string
		body map[string]any
	}
	requests := make(chan captured, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- captured{path: r.URL.Path, body: body}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	if got := run([]string{"control", "--server", server.URL, "--run", "r", "--task", "t", "pause"}); got != exitUsage {
		t.Fatalf("zero fence=%d", got)
	}
	if got := run([]string{"control", "--server", server.URL, "--run", "r", "--task", "t", "--fencing-token", "7", "pause"}); got != exitOK {
		t.Fatalf("control=%d", got)
	}
	control := <-requests
	if control.path != "/api/v1/runs/r/controls/pause" || control.body["fencing_token"] != float64(7) {
		t.Fatalf("control request=%+v", control)
	}

	base := []string{"reconcile", "--server", server.URL, "--run", "r", "--idempotency-key", "k", "--result", "observed"}
	if got := run(base); got != exitUsage {
		t.Fatalf("missing evidence=%d", got)
	}
	if got := run(append(base, "--evidence", "operator-check-42", "--disposition", "not_applied")); got != exitOK {
		t.Fatalf("reconcile=%d", got)
	}
	reconcile := <-requests
	if reconcile.path != "/api/v1/runs/r/reconcile" || reconcile.body["evidence"] != "operator-check-42" || reconcile.body["disposition"] != "not_applied" {
		t.Fatalf("reconcile request=%+v", reconcile)
	}
}

func TestRemoteBindFailsClosed(t *testing.T) {
	if got := run([]string{"serve", "--addr", "0.0.0.0:0", "--db", filepath.Join(t.TempDir(), "d.db")}); got != exitUsage {
		t.Fatalf("remote bind=%d", got)
	}
}

func TestRunOpenClawFlagsFailClosed(t *testing.T) {
	base := []string{"run", "--id", "r", "--catalog", "c", "--dag", "d"}
	if got := run(append(base, "--adapter", "unknown")); got != exitUsage {
		t.Fatalf("unknown adapter=%d", got)
	}
}
