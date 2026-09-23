package atomicx_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/internal/atomicx"
)

type I interface {
	Do()
}

type S struct{}

func (S) Do() {}

func TestAtomicValueStore(t *testing.T) {
	t.Parallel()

	var v atomicx.AtomicValue[I]
	require.Nil(t, v.Load())
	v.Store(nil)
	require.Nil(t, v.Load())
	v.Store(S{})
	require.NotNil(t, v.Load())
}

func TestAtomicValueZeroValue(t *testing.T) {
	t.Parallel()

	var s atomicx.AtomicValue[string]
	require.Equal(t, "", s.Load())

	var n atomicx.AtomicValue[int]
	require.Equal(t, 0, n.Load())
}

func TestAtomicValueSwap(t *testing.T) {
	t.Parallel()

	var v atomicx.AtomicValue[string]
	require.Equal(t, "", v.Swap("first"))
	require.Equal(t, "first", v.Swap("second"))
	require.Equal(t, "second", v.Load())
}

func TestAtomicValueLoadOrStore(t *testing.T) {
	t.Parallel()

	t.Run("stores once", func(t *testing.T) {
		t.Parallel()

		var v atomicx.AtomicValue[string]
		calls := 0
		store := func() (string, error) {
			calls++
			return "token", nil
		}

		got, err := v.LoadOrStore(store)
		require.NoError(t, err)
		require.Equal(t, "token", got)

		got, err = v.LoadOrStore(store)
		require.NoError(t, err)
		require.Equal(t, "token", got)
		require.Equal(t, 1, calls)
	})

	t.Run("propagates error and stores nothing", func(t *testing.T) {
		t.Parallel()

		var v atomicx.AtomicValue[string]
		boom := errors.New("boom")

		got, err := v.LoadOrStore(func() (string, error) { return "ignored", boom })
		require.ErrorIs(t, err, boom)
		require.Equal(t, "", got)
		require.Equal(t, "", v.Load())
	})

	t.Run("concurrent callers see one store", func(t *testing.T) {
		t.Parallel()

		var (
			v     atomicx.AtomicValue[string]
			mu    sync.Mutex
			calls int
			wg    sync.WaitGroup
		)
		for range 32 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				got, err := v.LoadOrStore(func() (string, error) {
					mu.Lock()
					calls++
					mu.Unlock()
					return "token", nil
				})
				require.NoError(t, err)
				require.Equal(t, "token", got)
			}()
		}
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()
		require.Equal(t, 1, calls)
	})
}
