package applicationfeed

import (
	"testing"
	"time"
)

func TestSourceGovernorBulkheadAndCircuitRecovery(t *testing.T) {
	governor := newSourceGovernor(1, 2, time.Second)
	if !governor.acquire() || governor.acquire() {
		t.Fatal("bulkhead did not enforce max in-flight requests")
	}
	governor.release()

	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	governor.now = func() time.Time { return now }
	governor.record(false)
	governor.record(false)
	if governor.allow() {
		t.Fatal("circuit should be open")
	}
	now = now.Add(time.Second)
	if !governor.allow() || governor.allow() {
		t.Fatal("circuit should allow only one half-open probe")
	}
	governor.record(true)
	if !governor.allow() {
		t.Fatal("successful probe should close circuit")
	}
}
