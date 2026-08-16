package dispatch

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestApprovalDigestBindsEverything(t *testing.T) {
	a := Action{Tool: "write", Target: "prod", Arguments: map[string]string{"x": "1"}, InputVersions: map[string]string{"prompt": "v1"}, ArtifactVersions: map[string]string{"doc": "sha"}, PolicyVersion: "p1", Scope: "release"}
	now := time.Now()
	ap := Approval{Digest: ActionDigest(a), Actor: "owner", Scope: "release", Expires: now.Add(time.Minute), AuthorizedActors: []string{"owner"}}
	if !ap.ValidFor(a, now) {
		t.Fatal("valid approval rejected")
	}
	mutations := []func(*Action){func(x *Action) { x.Target = "other" }, func(x *Action) { x.Arguments["x"] = "2" }, func(x *Action) { x.InputVersions["prompt"] = "v2" }, func(x *Action) { x.ArtifactVersions["doc"] = "other" }, func(x *Action) { x.PolicyVersion = "p2" }, func(x *Action) { x.Scope = "admin" }}
	for i, m := range mutations {
		x := a
		x.Arguments = clone(a.Arguments)
		x.InputVersions = clone(a.InputVersions)
		x.ArtifactVersions = clone(a.ArtifactVersions)
		m(&x)
		if ap.ValidFor(x, now) {
			t.Fatalf("mutation %d accepted", i)
		}
	}
	bad := ap
	bad.Actor = "intruder"
	if bad.ValidFor(a, now) {
		t.Fatal("unauthorized actor accepted")
	}
	if ap.ValidFor(a, ap.Expires) {
		t.Fatal("expiry boundary accepted")
	}
}
func clone(m map[string]string) map[string]string {
	o := map[string]string{}
	for k, v := range m {
		o[k] = v
	}
	return o
}

func TestCommandLifecycleIdempotencyAndReconciliation(t *testing.T) {
	c := NewController()
	now := time.Now()
	cmd := Command{ID: "c1", IdempotencyKey: "key", TaskID: "t"}
	got, err := c.Request(cmd, now)
	if err != nil || got.State != CommandRequested {
		t.Fatal(got, err)
	}
	again, err := c.Request(Command{ID: "different", IdempotencyKey: "key", TaskID: "t"}, now)
	if err != nil || again.ID != "c1" {
		t.Fatal("idempotency failed")
	}
	if _, err = c.Transition("key", CommandAcknowledged, "", "", now); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Transition("key", CommandOutcomeUnknown, "", "lost response", now); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Transition("key", CommandCompleted, "late", "", now); err == nil {
		t.Fatal("unknown outcome bypassed reconciliation")
	}
	got, err = c.Reconcile("key", "verified", now)
	if err != nil || got.State != CommandReconciled || got.Result != "verified" {
		t.Fatal(got, err)
	}
}

func TestExecuteOnceConcurrent(t *testing.T) {
	c := NewController()
	var n atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := c.ExecuteOnce("k", func() (string, error) { n.Add(1); return "ok", nil })
			if e != nil || v != "ok" {
				t.Errorf("%q %v", v, e)
			}
		}()
	}
	wg.Wait()
	if n.Load() != 1 {
		t.Fatalf("executed %d times", n.Load())
	}
}
func TestFencingEmergencyStopAndBound(t *testing.T) {
	c := NewBoundedController(1)
	old := c.RenewLease("t")
	fresh := c.RenewLease("t")
	if c.Commit("t", old) == nil {
		t.Fatal("stale fence accepted")
	}
	if err := c.Commit("t", fresh); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(Command{ID: "1", IdempotencyKey: "1", TaskID: "t"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Request(Command{ID: "2", IdempotencyKey: "2", TaskID: "t"}, time.Now()); err == nil {
		t.Fatal("runaway bound ignored")
	}
	c.EmergencyStop("operator")
	if err := c.Commit("t", fresh); err == nil {
		t.Fatal("commit after stop")
	}
	if stopped, reason := c.Stopped(); !stopped || reason != "operator" {
		t.Fatal(stopped, reason)
	}
}
func TestClassifyOutcome(t *testing.T) {
	if ClassifyTransportResult(true, false) != OutcomeUnknown {
		t.Fatal()
	}
	if ClassifyTransportResult(false, false) != OutcomeCompleted {
		t.Fatal()
	}
}

func TestLeaseExpiryHolderAndPersistence(t *testing.T) {
	now := time.Now()
	c := NewController()
	lease, err := c.AcquireLease("task", "worker-a", now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.AcquireLease("task", "worker-b", now, time.Second); err == nil {
		t.Fatal("lease stolen")
	}
	if err = c.CommitLease(lease, now.Add(time.Second)); err == nil {
		t.Fatal("expired lease committed")
	}
	fresh, err := c.AcquireLease("task", "worker-b", now.Add(time.Second), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.CommitLease(fresh, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	cmd := Command{ID: "c", IdempotencyKey: "k", TaskID: "task"}
	if _, err = c.Request(cmd, now); err != nil {
		t.Fatal(err)
	}
	snapshot := c.SnapshotCommands()
	restored := NewController()
	if err = restored.RestoreCommands(snapshot); err != nil {
		t.Fatal(err)
	}
	if got, ok := restored.Command("k"); !ok || got.ID != "c" {
		t.Fatal(got, ok)
	}
}
