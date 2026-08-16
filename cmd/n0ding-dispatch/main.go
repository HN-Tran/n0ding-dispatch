package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"github.com/hn-tran/n0ding-dispatch/internal/httpapi"
)

const (
	exitOK        = 0
	exitUsage     = 2
	exitTransport = 3
	exitRejected  = 4
)

func main()                    { os.Exit(run(os.Args[1:])) }
func emit(to io.Writer, v any) { _ = json.NewEncoder(to).Encode(v) }
func bad(code int, msg string) int {
	emit(os.Stderr, map[string]any{"ok": false, "error": msg, "exit_code": code})
	return code
}

func run(args []string) int {
	if len(args) == 0 {
		return bad(exitUsage, "command required: init|serve|run|runs|control|approve|reconcile|export|doctor")
	}
	switch args[0] {
	case "init":
		fs := flag.NewFlagSet("init", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		db := fs.String("db", "dispatch.db", "")
		if fs.Parse(args[1:]) != nil {
			return bad(exitUsage, "invalid init options")
		}
		s, e := core.OpenStore(*db)
		if e != nil {
			return bad(exitRejected, e.Error())
		}
		_ = s.Close()
		emit(os.Stdout, map[string]any{"ok": true, "database": *db})
		return 0
	case "serve":
		return serve(args[1:])
	case "doctor":
		fs, base, token := remoteFlags("doctor")
		if fs.Parse(args[1:]) != nil {
			return bad(exitUsage, "invalid doctor options")
		}
		return request(http.MethodGet, *base+"/healthz", *token, nil, os.Stdout)
	case "runs":
		fs, base, token := remoteFlags("runs")
		if fs.Parse(args[1:]) != nil {
			return bad(exitUsage, "invalid runs options")
		}
		return request(http.MethodGet, *base+"/api/v1/runs", *token, nil, os.Stdout)
	case "run":
		fs, base, token := remoteFlags("run")
		id := fs.String("id", "", "")
		name := fs.String("name", "", "")
		catalog := fs.String("catalog", "", "")
		dag := fs.String("dag", "", "")
		adapter := fs.String("adapter", "fixture", "")
		mode := fs.String("fixture-mode", "pass", "")
		timeout := fs.Int("timeout-ms", 15000, "")
		if fs.Parse(args[1:]) != nil || *id == "" || *catalog == "" || *dag == "" {
			return bad(exitUsage, "run requires --id --catalog --dag")
		}
		if *adapter != "fixture" && *adapter != "openclaw" {
			return bad(exitUsage, "--adapter must be fixture or openclaw")
		}
		return request("POST", *base+"/api/v1/dispatch/run", *token, map[string]any{"id": *id, "name": *name, "catalog_id": *catalog, "dag_id": *dag, "adapter": *adapter, "fixture_mode": *mode, "timeout_ms": *timeout}, os.Stdout)
	case "control":
		fs, base, token := remoteFlags("control")
		runID := fs.String("run", "", "")
		task := fs.String("task", "", "")
		key := fs.String("idempotency-key", "", "")
		fence := fs.Uint64("fencing-token", 0, "")
		agent := fs.String("agent", "", "")
		if fs.Parse(args[1:]) != nil || *runID == "" || fs.NArg() != 1 {
			return bad(exitUsage, "control requires --run RUN [--task TASK] [--fencing-token TOKEN] ACTION")
		}
		action := fs.Arg(0)
		if action != "emergency-stop" && *fence == 0 {
			return bad(exitUsage, "control action requires a non-zero --fencing-token")
		}
		if action == "reassign" && strings.TrimSpace(*agent) == "" {
			return bad(exitUsage, "reassign requires --agent")
		}
		return request("POST", fmt.Sprintf("%s/api/v1/runs/%s/controls/%s", *base, *runID, action), *token, map[string]any{"task_id": *task, "idempotency_key": *key, "fencing_token": *fence, "agent": *agent}, os.Stdout)
	case "approve":
		fs, base, token := remoteFlags("approve")
		runID := fs.String("run", "", "")
		digest := fs.String("digest", "", "")
		decision := fs.String("decision", "grant", "")
		if fs.Parse(args[1:]) != nil || *runID == "" || *digest == "" {
			return bad(exitUsage, "approve requires --run and --digest")
		}
		return request("POST", fmt.Sprintf("%s/api/v1/runs/%s/approvals/%s/%s", *base, *runID, *digest, *decision), *token, map[string]any{}, os.Stdout)
	case "reconcile":
		fs, base, token := remoteFlags("reconcile")
		runID := fs.String("run", "", "")
		key := fs.String("idempotency-key", "", "")
		result := fs.String("result", "", "")
		evidence := fs.String("evidence", "", "")
		disposition := fs.String("disposition", "", "")
		if fs.Parse(args[1:]) != nil || *runID == "" || *key == "" || strings.TrimSpace(*result) == "" || strings.TrimSpace(*evidence) == "" || (*disposition != "applied" && *disposition != "not_applied" && *disposition != "still_unknown") {
			return bad(exitUsage, "reconcile requires --run, --idempotency-key, --result, --evidence and --disposition")
		}
		return request("POST", fmt.Sprintf("%s/api/v1/runs/%s/reconcile", *base, *runID), *token, map[string]any{"idempotency_key": *key, "result": *result, "evidence": *evidence, "disposition": *disposition}, os.Stdout)
	case "export":
		fs, base, token := remoteFlags("export")
		runID := fs.String("run", "", "")
		if fs.Parse(args[1:]) != nil || *runID == "" {
			return bad(exitUsage, "export requires --run")
		}
		return request("GET", fmt.Sprintf("%s/api/v1/runs/%s/export", *base, *runID), *token, nil, os.Stdout)
	default:
		return bad(exitUsage, "unknown command")
	}
}
func remoteFlags(name string) (*flag.FlagSet, *string, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	base := fs.String("server", "http://127.0.0.1:8080", "")
	token := fs.String("token", os.Getenv("N0DING_DISPATCH_AUTH_TOKEN"), "")
	return fs, base, token
}
func request(method, url, token string, body any, out io.Writer) int {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, e := http.NewRequest(method, url, rd)
	if e != nil {
		return bad(exitUsage, e.Error())
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	c := http.Client{Timeout: 30 * time.Second}
	resp, e := c.Do(req)
	if e != nil {
		return bad(exitTransport, e.Error())
	}
	defer resp.Body.Close()
	_, _ = io.Copy(out, io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return exitRejected
	}
	return 0
}
func serve(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", "127.0.0.1:8080", "")
	db := fs.String("db", "dispatch.db", "")
	token := fs.String("auth-token", os.Getenv("N0DING_DISPATCH_AUTH_TOKEN"), "")
	openclawEndpoint := fs.String("openclaw-endpoint", "", "")
	if fs.Parse(args) != nil {
		return bad(exitUsage, "invalid serve options")
	}
	host, _, e := net.SplitHostPort(*addr)
	if e != nil {
		return bad(exitUsage, "invalid listen address")
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) && *token == "" {
		return bad(exitUsage, "non-loopback bind requires --auth-token")
	}
	if strings.ContainsAny(*token, "\r\n") {
		return bad(exitUsage, "invalid auth token")
	}
	store, e := core.OpenStore(*db)
	if e != nil {
		return bad(exitRejected, e.Error())
	}
	defer store.Close()
	emit(os.Stdout, map[string]any{"ok": true, "product": "n0ding-dispatch", "address": *addr})
	handler, e := httpapi.NewConfigured("dispatch", store, *token, *openclawEndpoint, os.Getenv("N0DING_DISPATCH_OPENCLAW_TOKEN"))
	if e != nil {
		return bad(exitUsage, e.Error())
	}
	server := http.Server{Addr: *addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 1 << 20}
	if e = server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
		return bad(exitTransport, e.Error())
	}
	return 0
}
