package applicationfeed

import (
	"context"
	"sync"
	"time"
)

type circuitState uint8

const (
	circuitClosed circuitState = iota
	circuitOpen
	circuitHalfOpen
)

// sourceGovernor 为推荐主召回提供进程内隔舱和熔断状态。
type sourceGovernor struct {
	slots     chan struct{}
	mu        sync.Mutex
	state     circuitState
	failures  int
	threshold int
	openUntil time.Time
	cooldown  time.Duration
	now       func() time.Time
}

func newSourceGovernor(maxInflight int, failureThreshold int, cooldown time.Duration) *sourceGovernor {
	if maxInflight <= 0 {
		maxInflight = 32
	}
	if failureThreshold <= 0 {
		failureThreshold = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	return &sourceGovernor{
		slots: make(chan struct{}, maxInflight), threshold: failureThreshold,
		cooldown: cooldown, now: time.Now,
	}
}

func (g *sourceGovernor) acquire() bool {
	if g == nil {
		return true
	}
	select {
	case g.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (g *sourceGovernor) release() {
	if g != nil {
		<-g.slots
	}
}

func (g *sourceGovernor) allow() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state == circuitOpen {
		if g.now().Before(g.openUntil) {
			return false
		}
		g.state = circuitHalfOpen
		return true
	}
	return g.state == circuitClosed
}

func (g *sourceGovernor) record(success bool) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if success {
		g.state = circuitClosed
		g.failures = 0
		return
	}
	if g.state == circuitHalfOpen {
		g.open()
		return
	}
	g.failures++
	if g.failures >= g.threshold {
		g.open()
	}
}

func (g *sourceGovernor) open() {
	g.state = circuitOpen
	g.openUntil = g.now().Add(g.cooldown)
}

func waitForCandidateRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
