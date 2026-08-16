package adapters

import (
	"context"
	"errors"
	"sync"
	"time"
)

type FixtureMode string

const (
	FixturePass         FixtureMode = "pass"
	FixtureFail         FixtureMode = "fail"
	FixtureTimeout      FixtureMode = "timeout"
	FixtureLostResponse FixtureMode = "lost_response"
)

// Fixture is deterministic and suitable for demos and conformance tests.
type Fixture struct {
	Mode          FixtureMode
	Delay         time.Duration
	SideEffecting bool
	mu            sync.Mutex
	tasks         map[string]string
}

func NewFixture(mode FixtureMode) *Fixture { return &Fixture{Mode: mode, tasks: map[string]string{}} }

func (f *Fixture) operation(ctx context.Context, op, id string, sideEffect bool) error {
	if f.Delay > 0 {
		select {
		case <-time.After(f.Delay):
		case <-ctx.Done():
			return &Error{Op: op, Err: ctx.Err(), Retryable: !sideEffect}
		}
	}
	switch f.Mode {
	case "", FixturePass:
		return nil
	case FixtureFail:
		return &Error{Op: op, Err: errors.New("fixture failure"), Retryable: true}
	case FixtureTimeout:
		<-ctx.Done()
		return &Error{Op: op, Err: ctx.Err(), Retryable: !sideEffect}
	case FixtureLostResponse:
		return &Error{Op: op, Err: errors.New("response lost"), Unknown: sideEffect, Retryable: !sideEffect}
	default:
		return &Error{Op: op, Err: errors.New("invalid fixture mode")}
	}
}

func (f *Fixture) Dispatch(ctx context.Context, r DispatchRequest) (Acknowledgement, error) {
	if err := f.operation(ctx, "dispatch", r.TaskID, true); err != nil {
		return Acknowledgement{}, err
	}
	f.mu.Lock()
	f.tasks[r.TaskID] = "running"
	f.mu.Unlock()
	return Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
func (f *Fixture) Heartbeat(ctx context.Context, r TaskRef) (Heartbeat, error) {
	if err := f.operation(ctx, "heartbeat", r.TaskID, false); err != nil {
		return Heartbeat{}, err
	}
	f.mu.Lock()
	state, ok := f.tasks[r.TaskID]
	f.mu.Unlock()
	return Heartbeat{TaskID: r.TaskID, Alive: ok, State: state}, nil
}
func (f *Fixture) Result(ctx context.Context, r TaskRef) (Result, error) {
	if err := f.operation(ctx, "result", r.TaskID, false); err != nil {
		return Result{}, err
	}
	f.mu.Lock()
	state := f.tasks[r.TaskID]
	f.mu.Unlock()
	if state == "" {
		state = "completed"
	}
	return Result{TaskID: r.TaskID, State: state, Output: map[string]any{"fixture": "ok"}}, nil
}
func (f *Fixture) Pause(ctx context.Context, r ControlRequest) (Acknowledgement, error) {
	if err := f.operation(ctx, "pause", r.TaskID, true); err != nil {
		return Acknowledgement{}, err
	}
	f.mu.Lock()
	f.tasks[r.TaskID] = "paused"
	f.mu.Unlock()
	return Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
func (f *Fixture) Cancel(ctx context.Context, r ControlRequest) (Acknowledgement, error) {
	if err := f.operation(ctx, "cancel", r.TaskID, true); err != nil {
		return Acknowledgement{}, err
	}
	f.mu.Lock()
	f.tasks[r.TaskID] = "cancelled"
	f.mu.Unlock()
	return Acknowledgement{TaskID: r.TaskID, Accepted: true}, nil
}
