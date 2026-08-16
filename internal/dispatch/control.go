package dispatch

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

type Action struct {
	Tool             string            `json:"tool"`
	Target           string            `json:"target"`
	Arguments        map[string]string `json:"arguments"`
	InputVersions    map[string]string `json:"input_versions,omitempty"`
	ArtifactVersions map[string]string `json:"artifact_versions,omitempty"`
	PolicyVersion    string            `json:"policy_version"`
	Scope            string            `json:"scope,omitempty"`
}

func ActionDigest(a Action) string {
	pairs := func(m map[string]string) [][2]string {
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]string, 0, len(keys))
		for _, k := range keys {
			out = append(out, [2]string{k, m[k]})
		}
		return out
	}
	b, _ := json.Marshal(struct {
		Tool, Target, Policy, Scope string
		Args, Inputs, Artifacts     [][2]string
	}{a.Tool, a.Target, a.PolicyVersion, a.Scope, pairs(a.Arguments), pairs(a.InputVersions), pairs(a.ArtifactVersions)})
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

type Approval struct {
	Digest, Actor, Scope string
	Expires              time.Time
	AuthorizedActors     []string
}

func (a Approval) ValidFor(action Action, now time.Time) bool {
	if a.Actor == "" || a.Scope == "" || action.Scope != a.Scope || !now.Before(a.Expires) || a.Digest != ActionDigest(action) {
		return false
	}
	for _, actor := range a.AuthorizedActors {
		if actor == a.Actor {
			return true
		}
	}
	return false
}

type CommandState string

const (
	CommandRequested      CommandState = "requested"
	CommandAcknowledged   CommandState = "acknowledged"
	CommandCompleted      CommandState = "completed"
	CommandFailed         CommandState = "failed"
	CommandOutcomeUnknown CommandState = "outcome_unknown"
	CommandReconciled     CommandState = "reconciled"
	CommandCancelled      CommandState = "cancelled"
	CommandPolling        CommandState = "polling"
)

type Command struct {
	ID             string       `json:"id"`
	IdempotencyKey string       `json:"idempotency_key"`
	TaskID         string       `json:"task_id"`
	State          CommandState `json:"state"`
	Fence          uint64       `json:"fence"`
	Result         string       `json:"result,omitempty"`
	Error          string       `json:"error,omitempty"`
	RequestedAt    time.Time    `json:"requested_at"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

type Lease struct {
	TaskID  string    `json:"task_id"`
	Token   uint64    `json:"token"`
	Holder  string    `json:"holder"`
	Expires time.Time `json:"expires"`
}

type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeUnknown   Outcome = "outcome_unknown"
)

func ClassifyTransportResult(sideEffecting, responseObserved bool) Outcome {
	if sideEffecting && !responseObserved {
		return OutcomeUnknown
	}
	return OutcomeCompleted
}

type Controller struct {
	mu           sync.Mutex
	results      map[string]string
	fences       map[string]uint64
	leases       map[string]Lease
	commands     map[string]Command
	stopped      bool
	stopReason   string
	maxCommands  int
	commandCount int
}

func NewController() *Controller { return NewBoundedController(10000) }
func NewBoundedController(max int) *Controller {
	if max <= 0 {
		max = 1
	}
	return &Controller{results: map[string]string{}, fences: map[string]uint64{}, leases: map[string]Lease{}, commands: map[string]Command{}, maxCommands: max}
}

func (c *Controller) Request(cmd Command, now time.Time) (Command, error) {
	result, _, err := c.RequestOnce(cmd, now)
	return result, err
}
func (c *Controller) RequestOnce(cmd Command, now time.Time) (Command, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return Command{}, false, errors.New("emergency stop active")
	}
	if cmd.ID == "" || cmd.IdempotencyKey == "" || cmd.TaskID == "" {
		return Command{}, false, errors.New("command id, task and idempotency key required")
	}
	if old, ok := c.commands[cmd.IdempotencyKey]; ok {
		return old, false, nil
	}
	if c.commandCount >= c.maxCommands {
		return Command{}, false, errors.New("runaway command bound reached")
	}
	cmd.State = CommandRequested
	cmd.RequestedAt = now
	cmd.UpdatedAt = now
	c.commands[cmd.IdempotencyKey] = cmd
	c.commandCount++
	return cmd, true, nil
}
func (c *Controller) Transition(key string, to CommandState, result, errText string, now time.Time) (Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.commands[key]
	if !ok {
		return Command{}, errors.New("unknown command")
	}
	allowed := map[CommandState]map[CommandState]bool{CommandRequested: {CommandAcknowledged: true, CommandFailed: true, CommandCancelled: true}, CommandAcknowledged: {CommandCompleted: true, CommandFailed: true, CommandOutcomeUnknown: true, CommandCancelled: true, CommandPolling: true}, CommandPolling: {CommandAcknowledged: true, CommandCompleted: true, CommandFailed: true, CommandCancelled: true}, CommandOutcomeUnknown: {CommandReconciled: true}}
	if !allowed[cmd.State][to] {
		return Command{}, errors.New("invalid command transition")
	}
	cmd.State = to
	cmd.Result = result
	cmd.Error = errText
	cmd.UpdatedAt = now
	c.commands[key] = cmd
	if to == CommandCompleted || to == CommandReconciled {
		c.results[key] = result
	}
	return cmd, nil
}
func (c *Controller) Reconcile(key, result string, now time.Time) (Command, error) {
	return c.Transition(key, CommandReconciled, result, "", now)
}
func (c *Controller) Command(key string) (Command, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.commands[key]
	return v, ok
}
func (c *Controller) RotateFenceForCommand(key, task string) (Command, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd, ok := c.commands[key]
	if !ok || cmd.TaskID != task {
		return Command{}, errors.New("unknown command")
	}
	c.fences[task]++
	cmd.Fence = c.fences[task]
	cmd.UpdatedAt = time.Now()
	c.commands[key] = cmd
	return cmd, nil
}

func (c *Controller) SnapshotCommands() []Command {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Command, 0, len(c.commands))
	for _, v := range c.commands {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IdempotencyKey < out[j].IdempotencyKey })
	return out
}
func (c *Controller) RestoreCommands(commands []Command) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cmd := range commands {
		if cmd.IdempotencyKey == "" || cmd.ID == "" || cmd.TaskID == "" {
			return errors.New("invalid persisted command")
		}
		if _, ok := c.commands[cmd.IdempotencyKey]; ok {
			return errors.New("duplicate persisted idempotency key")
		}
		c.commands[cmd.IdempotencyKey] = cmd
		if cmd.Fence > c.fences[cmd.TaskID] {
			c.fences[cmd.TaskID] = cmd.Fence
		}
		if cmd.State == CommandCompleted || cmd.State == CommandReconciled {
			c.results[cmd.IdempotencyKey] = cmd.Result
		}
		c.commandCount++
	}
	if c.commandCount > c.maxCommands {
		return errors.New("persisted commands exceed bound")
	}
	return nil
}

// RestoreFence raises the current fencing token. It never moves a fence
// backwards, which makes replaying persisted events safe and idempotent.
func (c *Controller) RestoreFence(task string, token uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if task == "" || token == 0 {
		return errors.New("task and non-zero fencing token required")
	}
	if token > c.fences[task] {
		c.fences[task] = token
	}
	return nil
}

func (c *Controller) ExecuteOnce(key string, fn func() (string, error)) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key == "" {
		return "", errors.New("idempotency key required")
	}
	if c.stopped {
		return "", errors.New("emergency stop active")
	}
	if v, ok := c.results[key]; ok {
		return v, nil
	}
	v, err := fn()
	if err != nil {
		return "", err
	}
	c.results[key] = v
	return v, nil
}
func (c *Controller) RenewLease(task string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fences[task]++
	return c.fences[task]
}
func (c *Controller) AcquireLease(task, holder string, now time.Time, ttl time.Duration) (Lease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return Lease{}, errors.New("emergency stop active")
	}
	if task == "" || holder == "" || ttl <= 0 {
		return Lease{}, errors.New("task, holder and positive ttl required")
	}
	if old, ok := c.leases[task]; ok && now.Before(old.Expires) && old.Holder != holder {
		return Lease{}, errors.New("lease held")
	}
	c.fences[task]++
	l := Lease{TaskID: task, Token: c.fences[task], Holder: holder, Expires: now.Add(ttl)}
	c.leases[task] = l
	return l, nil
}
func (c *Controller) CommitLease(lease Lease, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return errors.New("emergency stop active")
	}
	current, ok := c.leases[lease.TaskID]
	if !ok || current.Token != lease.Token || current.Holder != lease.Holder {
		return errors.New("stale fencing token")
	}
	if !now.Before(current.Expires) {
		return errors.New("lease expired")
	}
	return nil
}
func (c *Controller) Commit(task string, token uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return errors.New("emergency stop active")
	}
	if token == 0 || token != c.fences[task] {
		return errors.New("stale fencing token")
	}
	return nil
}
func (c *Controller) EmergencyStop(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	c.stopReason = reason
	for k, v := range c.fences {
		c.fences[k] = v + 1
	}
}

// RestoreEmergencyStop reconstructs the fail-closed stop state from the
// durable event log without generating new fencing tokens.
func (c *Controller) RestoreEmergencyStop(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	c.stopReason = reason
}
func (c *Controller) Stopped() (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped, c.stopReason
}
