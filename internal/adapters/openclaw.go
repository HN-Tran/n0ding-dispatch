package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

const maxBody = 1 << 20

// OpenClawHTTP implements the Dispatch adapter contract against an OpenClaw
// gateway. Token is sent only to the validated endpoint; redirects and ambient
// proxy configuration are deliberately disabled.
type OpenClawHTTP struct {
	base   *url.URL
	token  string
	client *http.Client
}

func NewOpenClawHTTP(endpoint, token string, timeout time.Duration) (*OpenClawHTTP, error) {
	u, err := validateEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	d := &net.Dialer{Timeout: min(timeout, 5*time.Second), KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy:                 nil,
		DialContext:           secureDialer(d),
		TLSHandshakeTimeout:   min(timeout, 5*time.Second),
		ResponseHeaderTimeout: timeout,
		MaxIdleConnsPerHost:   4,
	}
	client := &http.Client{Transport: tr, Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	return &OpenClawHTTP{base: u, token: token, client: client}, nil
}

func validateEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" {
		return nil, errors.New("invalid OpenClaw endpoint")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("endpoint must not contain credentials, query, or fragment")
	}
	loop := strings.EqualFold(u.Hostname(), "localhost") || net.ParseIP(u.Hostname()).IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && loop) {
		return nil, errors.New("HTTPS required except for loopback")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, errors.New("unsupported endpoint scheme")
	}
	u.Path = strings.TrimRight(u.EscapedPath(), "/")
	return u, nil
}

func secureDialer(d *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errors.New("endpoint resolved to no addresses")
		}
		loopName := strings.EqualFold(host, "localhost") || net.ParseIP(host).IsLoopback()
		for _, ip := range ips {
			if !allowedTargetIP(ip, loopName) {
				return nil, fmt.Errorf("blocked endpoint address %s", ip)
			}
		}
		// Dial a previously checked IP, not the hostname, to prevent DNS rebinding.
		return d.DialContext(ctx, network, net.JoinHostPort(ips[0].String(), port))
	}
}

func allowedTargetIP(ip net.IP, loopName bool) bool {
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	if ip.IsLoopback() {
		return loopName
	}
	return !ip.IsPrivate()
}

func (o *OpenClawHTTP) Dispatch(ctx context.Context, v DispatchRequest) (Acknowledgement, error) {
	var out Acknowledgement
	err := o.call(ctx, "dispatch", true, v, &out)
	return out, err
}
func (o *OpenClawHTTP) Heartbeat(ctx context.Context, v TaskRef) (Heartbeat, error) {
	var out Heartbeat
	err := o.call(ctx, "heartbeat", false, v, &out)
	return out, err
}
func (o *OpenClawHTTP) Result(ctx context.Context, v TaskRef) (Result, error) {
	var out Result
	err := o.call(ctx, "result", false, v, &out)
	return out, err
}
func (o *OpenClawHTTP) Pause(ctx context.Context, v ControlRequest) (Acknowledgement, error) {
	var out Acknowledgement
	err := o.call(ctx, "pause", true, v, &out)
	return out, err
}
func (o *OpenClawHTTP) Cancel(ctx context.Context, v ControlRequest) (Acknowledgement, error) {
	var out Acknowledgement
	err := o.call(ctx, "cancel", true, v, &out)
	return out, err
}

func (o *OpenClawHTTP) call(ctx context.Context, op string, sideEffect bool, input, output any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return &Error{Op: op, Err: err}
	}
	if len(b) > maxBody {
		return &Error{Op: op, Err: errors.New("request body too large")}
	}
	u := *o.base
	u.Path = strings.TrimRight(o.base.Path, "/") + "/api/v1/dispatch/" + op
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return &Error{Op: op, Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if o.token != "" {
		req.Header.Set("Authorization", "Bearer "+o.token)
	}
	var wrote atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) }}))
	resp, err := o.client.Do(req)
	if err != nil {
		unknown := sideEffect && wrote.Load()
		return &Error{Op: op, Err: err, Unknown: unknown, Retryable: !unknown}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody+1))
		return &Error{Op: op, Err: fmt.Errorf("gateway returned HTTP %d", resp.StatusCode), Retryable: resp.StatusCode == 429 || resp.StatusCode >= 500}
	}
	limited := io.LimitReader(resp.Body, maxBody+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return &Error{Op: op, Err: err, Unknown: sideEffect, Retryable: !sideEffect}
	}
	if len(data) > maxBody {
		return &Error{Op: op, Err: errors.New("response body too large")}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(output); err != nil {
		return &Error{Op: op, Err: fmt.Errorf("malformed response: %w", err)}
	}
	return nil
}
