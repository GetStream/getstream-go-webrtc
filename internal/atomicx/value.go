// Package atomicx provides small generic helpers over sync/atomic.
package atomicx

import (
	"sync"
	"sync/atomic"
)

// StoreFunc produces the value to store in AtomicValue.LoadOrStore.
type StoreFunc[T any] func() (T, error)

// AtomicValue is a lock-free container for a comparable value. The zero value
// is ready to use and Load returns the zero value of T until something is
// stored.
type AtomicValue[T comparable] struct {
	ptr atomic.Pointer[T]
	mu  sync.Mutex
}

func (v *AtomicValue[T]) Load() T {
	if p := v.ptr.Load(); p != nil {
		return *p
	}
	var zero T
	return zero
}

func (v *AtomicValue[T]) Store(val T) {
	v.ptr.Store(&val)
}

func (v *AtomicValue[T]) Swap(val T) T {
	if p := v.ptr.Swap(&val); p != nil {
		return *p
	}
	var zero T
	return zero
}

// LoadOrStore returns the stored value if it is non-zero, otherwise it calls
// store under a lock and keeps the result. store is called at most once per
// transition away from the zero value.
func (v *AtomicValue[T]) LoadOrStore(store StoreFunc[T]) (T, error) {
	var zero T
	if vv := v.Load(); vv != zero {
		return vv, nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	val := v.Load()
	if val == zero {
		var err error
		val, err = store()
		if err != nil {
			return zero, err
		}
		v.Store(val)
	}
	return val, nil
}
