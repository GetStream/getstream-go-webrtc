package event

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrAwaitTimeout = errors.New("await timeout")

// RemoveHandler undoes a handler registration.
type RemoveHandler func()

// Awaiter waits for a specific event.
type Awaiter[T any] interface {
	// Match reports whether the event satisfies the awaiter, in which case the
	// store drops it.
	Match(event T) bool
}

// EventAwaiter waits for an event of type T that matches a condition.
type EventAwaiter[T, Z any] struct {
	eventWatcherStore *Store[Z]
	event             chan T
	once              sync.Once
	matchFunc         func(T) bool
	timeout           atomic.Bool
}

// NewEventAwaiter creates and registers a new EventAwaiter.
func NewEventAwaiter[T, Z any](s *Store[Z], match func(T) bool) *EventAwaiter[T, Z] {
	e := &EventAwaiter[T, Z]{event: make(chan T, 32), matchFunc: match, eventWatcherStore: s}
	s.AddAwaiter(e)
	return e
}

// Match delivers the event to the waiting caller if it matches.
func (e *EventAwaiter[T, Z]) Match(event Z) bool {
	match := e.match(event)
	if match {
		e.Close()
	}
	return match
}

func (e *EventAwaiter[T, Z]) match(event Z) bool {
	// A timed-out awaiter reports a match so the store stops tracking it.
	if e.timeout.Load() {
		return true
	}
	if evt, ok := e.eventWatcherStore.mapFunc(event).(T); ok && e.matchFunc(evt) {
		e.event <- evt
		return true
	}
	return false
}

func (e *EventAwaiter[T, Z]) Close() {
	e.once.Do(func() {
		close(e.event)
	})
}

// Await waits for a matching event or for the deadline to expire.
func (e *EventAwaiter[T, Z]) Await(deadline time.Duration) (T, error) {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	select {
	case <-timer.C:
		e.timeout.Store(true)
		var zero T
		return zero, ErrAwaitTimeout
	case event := <-e.event:
		return event, nil
	}
}
