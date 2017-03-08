// Package leader implements lock based leader election.
package leader

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// LockValue represents a snapshot of the lock value and a method for updating it
// atomically.
type LockValue interface {
	// Get returns the value of the lock when the LockValue was created, or an
	// empty string if no value has been set yet.
	Get() (string, error)
	// Set declares that the lock value should be updated when Done is called.
	Set(string) error
	// Done frees any resources associated with a lock.
	Done() error
}

// Lock is an atomically writable value.
type Lock interface {
	// Value returns the current value of the lock.
	Value() (LockValue, error)
}

// Result is an observed result of an election.
type Result struct {
	// The ID of the current leader.
	LeaderID string
	// The time the current leader is leader until.
	Until time.Time
}

// Election is an instance of a leader election.
type Election struct {
	Lock Lock

	// ID is a required, unique identifier for the current process.
	ID string

	// Lease is the amount of time a leader attempts to acquire a lock for.
	// Defaults to 5 minutes.
	Lease time.Duration

	// Now is an optional method for determining the current time. It defaults
	// to time.Now.
	Now func() time.Time

	// OnLeader is called when a process has acquired leadership.
	OnLeader func()
	// OnFollower is called when a process has lost leadership.
	OnFollower func()
	// OnResult is called every time a result is successfully read from the lock.
	// It should NOT be used to determine leadership.
	OnResult func(Result)
	// OnError is called every time an error is experienced attempting to acquire
	// or read the lock.
	OnError func(error)
}

func (e *Election) onError(err error) {
	if e.OnError != nil {
		e.OnError(err)
	}
}

func (e *Election) onResult(r Result) {
	if e.OnResult != nil {
		e.OnResult(r)
	}
}

func (e *Election) onFollower() {
	if e.OnFollower != nil {
		e.OnFollower()
	}
}

func (e *Election) onLeader() {
	if e.OnLeader != nil {
		e.OnLeader()
	}
}

func (e *Election) now() time.Time {
	if e.Now == nil {
		return time.Now()
	}
	return e.Now()
}

func (e *Election) lease() time.Duration {
	if e.Lease == 0 {
		return time.Minute * 5
	}
	return e.Lease
}

// Start starts the election in a new goroutine.
func (e *Election) Start(ctx context.Context) {
	if e.ID == "" {
		panic("ID cannot be empty")
	}

	go e.start(ctx)
}

func backoff(currWait, maxWait time.Duration) time.Duration {
	return currWait + time.Duration(rand.Int63n(int64(maxWait-currWait)))
}

type record struct {
	LeaderID string    `json:"leaderID"`
	Until    time.Time `json:"until"`
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		<-t.C
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	stopTimer(t)
	t.Reset(d)
}

func (e *Election) start(ctx context.Context) {
	next := time.NewTimer(0)
	lease := time.NewTimer(0)
	stopTimer(next)
	stopTimer(lease)

	isLeader := false

	maxWait := time.Duration(float64(e.lease()) * 0.2)
	wait := time.Duration(0)

	for {
		next.Reset(wait)
		wait = backoff(wait, maxWait)

		select {
		case <-lease.C:
			isLeader = false
			e.onFollower()
		case <-next.C:
			r, updated, err := e.update()
			if err != nil {
				e.onError(err)
				continue
			}

			if updated {
				resetTimer(lease, r.Until.Sub(time.Now()))
				if !isLeader {
					e.onLeader()
				}
				isLeader = true
			}

			e.onResult(Result{LeaderID: r.LeaderID, Until: r.Until})
		case <-ctx.Done():
			// TODO(ericchiang): Un-acquire lock.
			return
		}
	}
}

func (e *Election) update() (r *record, updated bool, err error) {
	value, err := e.Lock.Value()
	if err != nil {
		return
	}
	defer func() {
		doneErr := value.Done()
		if err == nil && doneErr != nil {
			err = doneErr
		}
	}()

	curr, err := value.Get()
	if err != nil {
		return
	}

	if curr != "" {
		if err := json.Unmarshal([]byte(curr), r); err != nil {
			r = nil
		}
	}

	if r != nil && r.LeaderID != e.ID && r.Until.After(e.now()) {
		return
	}

	r = &record{LeaderID: e.ID, Until: e.now().Add(e.lease())}

	updatedValue, err := json.Marshal(r)
	if err != nil {
		return nil, false, fmt.Errorf("marshaling new value: %v", err)
	}

	if err := value.Set(string(updatedValue)); err != nil {
		return nil, false, err
	}
	return
}
