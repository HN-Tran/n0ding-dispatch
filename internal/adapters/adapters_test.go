package adapters

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFixtureContractAndLostResponse(t *testing.T) {
	f := NewFixture(FixturePass)
	ack, err := f.Dispatch(context.Background(), DispatchRequest{TaskID: "t1"})
	if err != nil || !ack.Accepted {
		t.Fatalf("dispatch: %#v %v", ack, err)
	}
	hb, _ := f.Heartbeat(context.Background(), TaskRef{TaskID: "t1"})
	if !hb.Alive || hb.State != "running" {
		t.Fatalf("heartbeat: %#v", hb)
	}
	if _, err := f.Pause(context.Background(), ControlRequest{TaskID: "t1"}); err != nil {
		t.Fatal(err)
	}
	res, _ := f.Result(context.Background(), TaskRef{TaskID: "t1"})
	if res.State != "paused" {
		t.Fatalf("result: %#v", res)
	}

	lost := NewFixture(FixtureLostResponse)
	_, err = lost.Dispatch(context.Background(), DispatchRequest{TaskID: "t2"})
	if !IsOutcomeUnknown(err) || IsRetryable(err) {
		t.Fatalf("side effect must be unknown and non-retryable: %v", err)
	}
	_, err = lost.Result(context.Background(), TaskRef{TaskID: "t2"})
	if IsOutcomeUnknown(err) || !IsRetryable(err) {
		t.Fatalf("read must be retryable: %v", err)
	}
}

func TestFixtureTimeoutAndCancellation(t *testing.T) {
	f := NewFixture(FixtureTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := f.Heartbeat(ctx, TaskRef{TaskID: "t"})
	if !errors.Is(err, context.DeadlineExceeded) || !IsRetryable(err) {
		t.Fatalf("timeout: %v", err)
	}

	f = NewFixture(FixturePass)
	f.Delay = time.Second
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, err = f.Dispatch(ctx, DispatchRequest{TaskID: "t"})
	if !errors.Is(err, context.Canceled) || IsRetryable(err) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestEndpointValidation(t *testing.T) {
	bad := []string{"", "http://example.com", "ftp://example.com", "https://user:pass@example.com", "https://example.com?q=x"}
	for _, raw := range bad {
		if _, err := NewOpenClawHTTP(raw, "", time.Second); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	if _, err := NewOpenClawHTTP("http://127.0.0.1:8080", "", time.Second); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"https://10.0.0.1:443", "https://169.254.1.1:443"} {
		a, err := NewOpenClawHTTP(raw, "", 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		_, err = a.Heartbeat(context.Background(), TaskRef{TaskID: "t"})
		if err == nil || !strings.Contains(err.Error(), "blocked endpoint") {
			t.Errorf("%s: %v", raw, err)
		}
	}
}

func TestOpenClawHTTPContractMalformedAndClassification(t *testing.T) {
	var auth string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		if r.URL.Path != "/api/v1/dispatch/dispatch" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"task_id":"t","accepted":true}`))
	}))
	defer s.Close()
	a, err := NewOpenClawHTTP(s.URL, "secret", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := a.Dispatch(context.Background(), DispatchRequest{TaskID: "t"})
	if err != nil || !ack.Accepted || auth != "Bearer secret" {
		t.Fatalf("ack=%#v auth=%q err=%v", ack, auth, err)
	}

	malformed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{`)) }))
	defer malformed.Close()
	a, _ = NewOpenClawHTTP(malformed.URL, "", time.Second)
	if _, err = a.Result(context.Background(), TaskRef{}); err == nil || !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("malformed: %v", err)
	}

	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", 503) }))
	defer unavailable.Close()
	a, _ = NewOpenClawHTTP(unavailable.URL, "", time.Second)
	_, err = a.Result(context.Background(), TaskRef{})
	if !IsRetryable(err) || IsOutcomeUnknown(err) {
		t.Fatalf("503 classification: %v", err)
	}
}

func TestOpenClawTimeoutCancellationAndLostSideEffect(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"task_id":"t","state":"late"}`))
	}))
	defer slow.Close()
	a, _ := NewOpenClawHTTP(slow.URL, "", 30*time.Millisecond)
	_, err := a.Result(context.Background(), TaskRef{})
	if err == nil || !IsRetryable(err) {
		t.Fatalf("timeout: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = a.Result(ctx, TaskRef{})
	if !errors.Is(err, context.Canceled) || !IsRetryable(err) {
		t.Fatalf("cancel: %v", err)
	}

	lost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h, ok := w.(http.Hijacker)
		if !ok {
			panic("no hijacker")
		}
		conn, _, e := h.Hijack()
		if e != nil {
			panic(e)
		}
		_ = conn.Close()
	}))
	defer lost.Close()
	a, _ = NewOpenClawHTTP(lost.URL, "", time.Second)
	_, err = a.Dispatch(context.Background(), DispatchRequest{TaskID: "t"})
	if !IsOutcomeUnknown(err) || IsRetryable(err) {
		t.Fatalf("lost response: %v", err)
	}
}

func TestBoundedResponse(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, strings.Repeat("x", maxBody+1)) }))
	defer s.Close()
	a, _ := NewOpenClawHTTP(s.URL, "", time.Second)
	_, err := a.Result(context.Background(), TaskRef{})
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("large response: %v", err)
	}
}
