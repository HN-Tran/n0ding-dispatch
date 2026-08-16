package httpapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
)

func TestRunLifecycleAndModeIsolation(t *testing.T) {
	s := core.NewStore()
	h := New("dispatch", s)
	if err := s.CreateRun(core.Run{ID: "one", Name: "One", Mode: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	if _, err := s.Append("one", "dispatch.completed", map[string]any{"password": "nope"}); err != nil {
		t.Fatal(err)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runs/one/projection", nil))
	if !strings.Contains(w.Body.String(), `"status":"completed"`) {
		t.Fatalf("projection: %s", w.Body.String())
	}
	_ = s.CreateRun(core.Run{ID: "other", Mode: "foreign"})
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runs", nil))
	if strings.Contains(w.Body.String(), "other") {
		t.Fatalf("mode leak: %s", w.Body.String())
	}
}

func TestEventsResumeJSON(t *testing.T) {
	s := core.NewStore()
	_ = s.CreateRun(core.Run{ID: "r", Mode: "dispatch"})
	a, _ := s.Append("r", "event.one", nil)
	b, _ := s.Append("r", "event.two", nil)
	h := New("dispatch", s)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runs/r/events?after="+json.Number(string(rune('0'+a.ID))).String(), nil))
	var out struct {
		Events []core.Event `json:"events"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if len(out.Events) != 1 || out.Events[0].ID != b.ID {
		t.Fatalf("resume failed: %s", w.Body.String())
	}
}

func TestSSEResumesFromLastEventID(t *testing.T) {
	s := core.NewStore()
	_ = s.CreateRun(core.Run{ID: "r", Mode: "dispatch"})
	first, _ := s.Append("r", "event.one", nil)
	second, _ := s.Append("r", "event.two", nil)
	srv := httptest.NewServer(New("dispatch", s))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/api/v1/runs/r/events", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", json.Number(string(rune('0'+first.ID))).String())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	rd := bufio.NewReader(resp.Body)
	var buf bytes.Buffer
	for {
		line, err := rd.ReadString('\n')
		buf.WriteString(line)
		if strings.HasPrefix(line, "data:") {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	got := buf.String()
	if strings.Contains(got, `"type":"event.one"`) || !strings.Contains(got, `"event_id":"`+strconv.FormatInt(second.ID, 10)+`"`) {
		t.Fatalf("bad resumed SSE: %s", got)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
}

func TestAuthModeIsolationAndCursorValidation(t *testing.T) {
	s := core.NewStore()
	_ = s.CreateRun(core.Run{ID: "dispatch-only", Mode: "foreign"})
	h := NewAuthenticated("dispatch", s, "secret")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runs", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth: %d", w.Code)
	}
	req := httptest.NewRequest("GET", "/api/v1/runs/dispatch-only/events", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-mode access: %d %s", w.Code, w.Body.String())
	}
	_ = s.CreateRun(core.Run{ID: "other-only", Mode: "dispatch"})
	req = httptest.NewRequest("GET", "/api/v1/runs/other-only/events?after=-1", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor accepted: %d", w.Code)
	}
}

func TestSecretAbsentFromAPIAndExport(t *testing.T) {
	s := core.NewStore()
	h := New("dispatch", s)
	w := httptest.NewRecorder()
	if err := s.CreateRun(core.Run{ID: "safe", Name: "sentinel-supersecret", Mode: "dispatch"}); err != nil {
		t.Fatal(err)
	}
	_, _ = s.Append("safe", "dispatch.started", map[string]any{"message": "sentinel-supersecret", "api_key": "sentinel-supersecret"})
	for _, path := range []string{"/api/v1/runs/safe/events", "/api/v1/runs/safe/export"} {
		w = httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if strings.Contains(w.Body.String(), "sentinel-supersecret") {
			t.Fatalf("secret in %s", path)
		}
	}
}

func TestEventBacklogIsBounded(t *testing.T) {
	s := core.NewStore()
	_ = s.CreateRun(core.Run{ID: "r", Mode: "dispatch"})
	for i := 0; i < maxSSEBacklog+1; i++ {
		_, _ = s.Append("r", "task.completed", map[string]any{"n": i})
	}
	w := httptest.NewRecorder()
	New("dispatch", s).ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/runs/r/events", nil))
	if w.Code != http.StatusConflict {
		t.Fatalf("unbounded backlog returned: %d", w.Code)
	}
}
