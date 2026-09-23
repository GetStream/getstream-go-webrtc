// Package event provides a small pub/sub store for intercepting events and
// waiting for a specific one to arrive.
package event

import "sync"

// Store manages a collection of interceptors and awaiters for events of type T.
type Store[T any] struct {
	mapFunc       func(T) any // Maps events to the value awaiters match against.
	mu            sync.RWMutex
	eventAwaiters []Awaiter[T]
	interceptors  []interceptor[T]
	nextID        uint64
}

// interceptor pairs a callback with the identity the store removes it by.
type interceptor[T any] struct {
	fn func(T)
	id uint64
}

// NewStore creates a new Store with the provided mapping function.
func NewStore[T any](m func(T) any) *Store[T] {
	return &Store[T]{mapFunc: m}
}

// AddAwaiter adds an awaiter to the store.
func (s *Store[T]) AddAwaiter(a Awaiter[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventAwaiters = append(s.eventAwaiters, a)
}

// AddInterceptor adds an interceptor to the store and returns the function that
// removes it again.
//
// Removal is by the returned handle rather than by the callback value, because
// func values cannot be compared: every closure produced by one func literal
// shares that literal's code pointer, so the handlers HandleEvent registers are
// indistinguishable from each other.
func (s *Store[T]) AddInterceptor(f func(T)) RemoveHandler {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextID++
	id := s.nextID
	s.interceptors = append(s.interceptors, interceptor[T]{fn: f, id: id})
	return func() { s.removeInterceptor(id) }
}

func (s *Store[T]) removeInterceptor(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, in := range s.interceptors {
		if in.id == id {
			s.interceptors = append(s.interceptors[:i], s.interceptors[i+1:]...)
			return
		}
	}
}

// Intercept passes an event to every interceptor, then to every awaiter,
// dropping the awaiters that matched.
func (s *Store[T]) Intercept(event T) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, in := range s.interceptors {
		in.fn(event)
	}

	remaining := make([]Awaiter[T], 0, len(s.eventAwaiters))
	for _, awaiter := range s.eventAwaiters {
		if !awaiter.Match(event) {
			remaining = append(remaining, awaiter)
		}
	}
	s.eventAwaiters = remaining
}
