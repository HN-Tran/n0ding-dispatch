package main

import (
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
	if got := run(append(base, "--adapter", "openclaw", "--openclaw-endpoint", "http://127.0.0.1:1")); got != exitUsage {
		t.Fatalf("missing token env=%d", got)
	}
}
