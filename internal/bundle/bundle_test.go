package bundle

import (
	"encoding/json"
	"github.com/hn-tran/n0ding-dispatch/internal/core"
	"testing"
)

func TestExportVerifyReplayAndTamper(t *testing.T) {
	s := core.NewStore()
	core.LoadFixture(s, "dispatch")
	raw, err := Export(s, "dispatch-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = VerifyAndReplay(raw); err != nil {
		t.Fatal(err)
	}
	for i := range raw {
		if raw[i] == 'b' {
			raw[i] = 'x'
			break
		}
	}
	if _, err = VerifyAndReplay(raw); err == nil {
		t.Fatal("tampered bundle accepted")
	}
}

func TestSequenceGapRejectedEvenWithUpdatedChecksum(t *testing.T) {
	s := core.NewStore()
	core.LoadFixture(s, "dispatch")
	raw, _ := Export(s, "dispatch-fixture")
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	b.Events[1].Sequence = 9
	b.Manifest.EventsDigest, _ = digest(b.Events)
	raw, _ = json.Marshal(b)
	if _, err := VerifyAndReplay(raw); err == nil {
		t.Fatal("sequence gap accepted")
	}
}
