package adapters

import (
	"context"
	"errors"
)

// Adapter is the deliberately small boundary between Dispatch and an agent
// runtime. Implementations must not retry side-effecting operations themselves.
type Adapter interface {
	Dispatch(context.Context, DispatchRequest) (Acknowledgement, error)
	Heartbeat(context.Context, TaskRef) (Heartbeat, error)
	Result(context.Context, TaskRef) (Result, error)
	Pause(context.Context, ControlRequest) (Acknowledgement, error)
	Cancel(context.Context, ControlRequest) (Acknowledgement, error)
}

type DispatchRequest struct {
	TaskID         string         `json:"task_id"`
	Agent          string         `json:"agent"`
	Payload        map[string]any `json:"payload,omitempty"`
	IdempotencyKey string         `json:"idempotency_key"`
	FencingToken   uint64         `json:"fencing_token"`
}

type TaskRef struct {
	TaskID string `json:"task_id"`
}

type ControlRequest struct {
	TaskID         string `json:"task_id"`
	IdempotencyKey string `json:"idempotency_key"`
	FencingToken   uint64 `json:"fencing_token"`
}

type Acknowledgement struct {
	TaskID   string `json:"task_id"`
	Accepted bool   `json:"accepted"`
}

type Heartbeat struct {
	TaskID string `json:"task_id"`
	Alive  bool   `json:"alive"`
	State  string `json:"state,omitempty"`
}

type Result struct {
	TaskID string         `json:"task_id"`
	State  string         `json:"state"`
	Output map[string]any `json:"output,omitempty"`
}

type Outcome string

const (
	OutcomeFailed  Outcome = "failed"
	OutcomeUnknown Outcome = "outcome_unknown"
)

// Error preserves the information callers need to make safe retry decisions.
// Unknown is true when a side effect may have happened but no response arrived.
type Error struct {
	Op        string
	Err       error
	Retryable bool
	Unknown   bool
}

func (e *Error) Error() string { return e.Op + ": " + e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

func IsRetryable(err error) bool      { var e *Error; return errors.As(err, &e) && e.Retryable }
func IsOutcomeUnknown(err error) bool { var e *Error; return errors.As(err, &e) && e.Unknown }
