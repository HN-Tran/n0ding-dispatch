package httpapi

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/adapters"
	"github.com/hn-tran/n0ding-dispatch/internal/bundle"
	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/dispatch"
	webassets "github.com/hn-tran/n0ding-dispatch/web"
)

type Server struct {
	Mode           string
	Store          *core.Store
	Token          string
	mu             sync.Mutex
	catalogs       map[string]dispatch.Catalog
	dags           map[string]dispatch.TaskDAG
	controllers    map[string]*dispatch.Controller
	adapters       map[string]adapters.Adapter
	runTimeouts    map[string]time.Duration
	approvalClaims map[string]bool
	openclaw       adapters.Adapter
	mutations      []time.Time
	fatalErr       string
}

const maxBodyBytes int64 = 1 << 20
const maxSSEBacklog = 1000

func New(mode string, store *core.Store) http.Handler {
	return NewAuthenticated(mode, store, "")
}

func NewAuthenticated(mode string, store *core.Store, token string) http.Handler {
	h, _ := NewConfigured(mode, store, token, "", "")
	return h
}

// NewConfigured binds the OpenClaw credential to a server-owned endpoint.
// Run requests can select this adapter but cannot choose where its secret goes.
func NewConfigured(mode string, store *core.Store, token, openclawEndpoint, openclawToken string) (http.Handler, error) {
	var openclaw adapters.Adapter
	if openclawEndpoint != "" || openclawToken != "" {
		if openclawEndpoint == "" || openclawToken == "" {
			return nil, fmt.Errorf("OpenClaw endpoint and token must be configured together")
		}
		var err error
		openclaw, err = adapters.NewOpenClawHTTP(openclawEndpoint, openclawToken, 15*time.Second)
		if err != nil {
			return nil, err
		}
	}
	s := &Server{Mode: mode, Store: store, Token: token, catalogs: map[string]dispatch.Catalog{}, dags: map[string]dispatch.TaskDAG{}, controllers: map[string]*dispatch.Controller{}, adapters: map[string]adapters.Adapter{}, runTimeouts: map[string]time.Duration{}, approvalClaims: map[string]bool{}, openclaw: openclaw}
	s.loadDefinitions()
	s.recoverControllers()
	s.recoverAdapters()
	s.recoverInterrupted()
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", s.health)
	m.HandleFunc("GET /api/v1/runs", s.listRuns)
	m.HandleFunc("GET /api/v1/runs/{id}/events", s.events)
	m.HandleFunc("GET /api/v1/runs/{id}/projection", s.projection)
	m.HandleFunc("POST /api/v1/fixtures", s.fixture)
	m.HandleFunc("GET /api/v1/runs/{id}/export", s.exportRun)
	m.HandleFunc("POST /api/v1/replay/import", s.importReplay)
	m.HandleFunc("GET /api/v1/agents", s.listAgents)
	m.HandleFunc("POST /api/v1/agents", s.putCatalog)
	m.HandleFunc("GET /api/v1/tasks", s.listTasks)
	m.HandleFunc("POST /api/v1/tasks", s.putDAG)
	m.HandleFunc("POST /api/v1/dispatch/run", s.startDispatch)
	m.HandleFunc("GET /api/v1/runs/{id}/decisions", s.decisions)
	m.HandleFunc("GET /api/v1/runs/{id}/approvals", s.approvals)
	m.HandleFunc("GET /api/v1/runs/{id}/artifacts", s.artifacts)
	m.HandleFunc("GET /api/v1/runs/{id}/messages", s.messages)
	m.HandleFunc("POST /api/v1/runs/{id}/controls/{action}", s.control)
	m.HandleFunc("POST /api/v1/runs/{id}/approvals/{digest}/{decision}", s.approve)
	m.HandleFunc("POST /api/v1/runs/{id}/reconcile", s.reconcile)
	m.HandleFunc("POST /api/v1/runs/{id}/tasks/{task}/result", s.taskResult)
	m.Handle("GET /", http.FileServer(http.FS(webassets.FS)))
	return s.security(m), nil
}

func (s *Server) exportRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.owns(id) {
		write(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	raw, err := bundle.Export(s.Store, id)
	if err != nil {
		write(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="n0ding-replay.json"`)
	w.Write(raw)
}
func (s *Server) importReplay(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, bundle.MaxBundleBytes)
	defer r.Body.Close()
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": "invalid or oversized replay bundle"})
		return
	}
	p, err := bundle.VerifyAndReplay(raw)
	if err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	write(w, http.StatusOK, map[string]any{"mode": "replay", "projection": p})
}

func (s *Server) security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		if s.Token != "" && strings.HasPrefix(r.URL.Path, "/api/") {
			authorization := r.Header.Get("Authorization")
			if !strings.HasPrefix(authorization, "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(authorization, "Bearer ")), []byte(s.Token)) != 1 {
				write(w, http.StatusUnauthorized, map[string]string{"error": "authentication required"})
				return
			}
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead && strings.HasPrefix(r.URL.Path, "/api/") {
			if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
				write(w, http.StatusForbidden, map[string]string{"error": "cross-site mutation rejected"})
				return
			}
			if origin := r.Header.Get("Origin"); origin != "" && origin != "http://"+r.Host && origin != "https://"+r.Host {
				write(w, http.StatusForbidden, map[string]string{"error": "cross-origin mutation rejected"})
				return
			}
			if !s.allowMutation() {
				write(w, http.StatusTooManyRequests, map[string]string{"error": "mutation rate limit exceeded"})
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) allowMutation() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	cut := now.Add(-time.Minute)
	keep := s.mutations[:0]
	for _, t := range s.mutations {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	s.mutations = keep
	if len(keep) >= 120 {
		return false
	}
	s.mutations = append(s.mutations, now)
	return true
}

func (s *Server) owns(id string) bool { run, ok := s.Store.GetRun(id); return ok && run.Mode == s.Mode }

func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	fatal := s.fatalErr
	s.mu.Unlock()
	if fatal != "" {
		write(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "mode": s.Mode, "error": "durable event append failed"})
		return
	}
	write(w, http.StatusOK, map[string]any{"ok": true, "mode": s.Mode})
}

func (s *Server) appendCritical(runID, typ string, data map[string]any) (core.Event, error) {
	e, err := s.Store.Append(runID, typ, data)
	if err != nil {
		s.mu.Lock()
		s.fatalErr = err.Error()
		s.mu.Unlock()
	}
	return e, err
}

func (s *Server) listRuns(w http.ResponseWriter, _ *http.Request) {
	write(w, http.StatusOK, map[string]any{"runs": s.Store.ListRuns(s.Mode)})
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var x struct{ ID, Name string }
	if json.NewDecoder(r.Body).Decode(&x) != nil || x.ID == "" {
		write(w, http.StatusBadRequest, map[string]string{"error": "id required"})
		return
	}
	run := core.Run{ID: x.ID, Name: x.Name, Mode: s.Mode}
	if err := s.Store.CreateRun(run); err != nil {
		write(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	run, _ = s.Store.GetRun(x.ID)
	write(w, http.StatusCreated, run)
}

func (s *Server) appendEvent(w http.ResponseWriter, r *http.Request) {
	if !s.owns(r.PathValue("id")) {
		write(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var x struct {
		Type string         `json:"type"`
		Data map[string]any `json:"data"`
	}
	if json.NewDecoder(r.Body).Decode(&x) != nil || x.Type == "" {
		write(w, http.StatusBadRequest, map[string]string{"error": "type required"})
		return
	}
	e, err := s.Store.Append(r.PathValue("id"), x.Type, x.Data)
	if err != nil {
		write(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	write(w, http.StatusCreated, e)
}

func afterID(r *http.Request) (int64, error) {
	raw := r.URL.Query().Get("after")
	if raw == "" {
		raw = r.Header.Get("Last-Event-ID")
	}
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid event cursor")
	}
	return n, nil
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.owns(id) {
		write(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	cursor, err := afterID(r)
	if err != nil {
		write(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if !strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		events, more := s.Store.EventsLimit(id, cursor, maxSSEBacklog)
		if more {
			write(w, http.StatusConflict, map[string]string{"error": "event backlog exceeds bounded response; advance cursor"})
			return
		}
		write(w, http.StatusOK, map[string]any{"events": events})
		return
	}
	if _, more := s.Store.EventsLimit(id, cursor, maxSSEBacklog); more {
		write(w, http.StatusConflict, map[string]string{"error": "event backlog exceeds stream limit; resync from snapshot"})
		return
	}
	f, ok := w.(http.Flusher)
	if !ok {
		write(w, http.StatusInternalServerError, map[string]string{"error": "stream unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	send := func() bool {
		events, more := s.Store.EventsLimit(id, cursor, maxSSEBacklog)
		if more {
			return false
		}
		for _, e := range events {
			b, _ := json.Marshal(e)
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.ID, e.Type, b); err != nil {
				return false
			}
			cursor = e.ID
		}
		f.Flush()
		return true
	}
	for {
		ch, _ := s.Store.Subscribe()
		if !send() {
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			continue
		case <-time.After(15 * time.Second):
			fmt.Fprint(w, ": keepalive\n\n")
			f.Flush()
		}
	}
}

func (s *Server) projection(w http.ResponseWriter, r *http.Request) {
	if !s.owns(r.PathValue("id")) {
		write(w, http.StatusNotFound, map[string]string{"error": "run not found"})
		return
	}
	upto, _ := strconv.ParseInt(r.URL.Query().Get("upto"), 10, 64)
	p, err := s.Store.Replay(r.PathValue("id"), upto)
	if err != nil {
		write(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	write(w, http.StatusOK, p)
}

func (s *Server) fixture(w http.ResponseWriter, _ *http.Request) {
	run, err := core.LoadFixture(s.Store, s.Mode)
	if err != nil {
		write(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	write(w, http.StatusCreated, run)
}
