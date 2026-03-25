/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package router

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	actionerfake "github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner/fake"
)

// runRouter creates and runs a real router loop, returning the loop and a
// channel that receives the loop's exit error. The caller must eventually
// close the router to avoid leaking goroutines.
func runRouter(t *testing.T, ctx context.Context) (*routerHandle, <-chan error) {
	t.Helper()

	cancelCh := make(chan error, 1)
	routerLoop := New(Options{
		Actioner: actionerfake.New(),
		Log:      logr.Discard(),
		Cancel: func(err error) {
			select {
			case cancelCh <- err:
			default:
			}
		},
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- routerLoop.Run(ctx)
	}()

	return &routerHandle{loop: routerLoop, cancelCh: cancelCh}, errCh
}

type routerHandle struct {
	loop     Interface
	cancelCh <-chan error
}

// Interface is a local alias to avoid import cycle with the loop package.
type Interface = interface {
	Run(ctx context.Context) error
	Enqueue(event *queue.JobEvent)
	Close(event *queue.JobEvent)
}

func (h *routerHandle) enqueue(event *queue.JobEvent) {
	h.loop.Enqueue(event)
}

func (h *routerHandle) close() {
	h.loop.Close(&queue.JobEvent{
		Action: &queue.JobAction{
			Action: &queue.JobAction_Close{Close: new(queue.Close)},
		},
	})
}

func closeJobEvent(name string) *queue.JobEvent {
	return &queue.JobEvent{
		JobName: name,
		Action: &queue.JobAction{
			Action: &queue.JobAction_CloseJob{CloseJob: new(queue.CloseJob)},
		},
	}
}

func informPutEvent(name string) *queue.JobEvent {
	return &queue.JobEvent{
		JobName: name,
		Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{Name: name, IsPut: true},
			},
		},
	}
}

func informDeleteEvent(name string) *queue.JobEvent {
	return &queue.JobEvent{
		JobName: name,
		Action: &queue.JobAction{
			Action: &queue.JobAction_Informed{
				Informed: &queue.Informed{Name: name, IsPut: false},
			},
		},
	}
}

// waitClean closes the router and asserts it exited cleanly.
func waitClean(t *testing.T, h *routerHandle, errCh <-chan error) {
	t.Helper()

	h.close()

	select {
	case err := <-errCh:
		require.NoError(t, err, "router loop should exit cleanly")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for router loop to exit")
	}

	select {
	case err := <-h.cancelCh:
		t.Fatalf("r.cancel was called: %v — scheduler would crash", err)
	default:
	}
}

// Test_router_integration must NOT be t.Parallel() — the real router uses
// counters.LoopsCache (a package-level sync.Pool). If unit tests run
// concurrently, their fake loops can leak into the pool and be reused by
// the integration test's real router, causing data races.
func Test_router_integration(t *testing.T) {

	t.Run("CloseJob for missing counter does not kill the router loop", func(t *testing.T) {

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		h, errCh := runRouter(t, ctx)

		h.enqueue(closeJobEvent("nonexistent"))

		h.enqueue(informPutEvent("sentinel"))

		waitClean(t, h, errCh)
	})

	t.Run("many ghost CloseJobs followed by real work", func(t *testing.T) {

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		h, errCh := runRouter(t, ctx)

		for i := range 50 {
			h.enqueue(closeJobEvent("ghost-" + strconv.Itoa(i)))
		}

		h.enqueue(informPutEvent("alive"))

		waitClean(t, h, errCh)
	})

	t.Run("duplicate CloseJob after counter already removed", func(t *testing.T) {

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		h, errCh := runRouter(t, ctx)

		// Create a counter, then delete it (which causes close → CloseJob).
		h.enqueue(informPutEvent("dup"))
		h.enqueue(informDeleteEvent("dup"))

		// First CloseJob — removes counter from router map.
		h.enqueue(closeJobEvent("dup"))

		// Second CloseJob — counter is gone. Before the fix, this crashes.
		h.enqueue(closeJobEvent("dup"))

		// Verify alive.
		h.enqueue(informPutEvent("post-dup"))

		waitClean(t, h, errCh)
	})

	t.Run("namespace deletion pattern: create, delete-inform, CloseJob, ghost CloseJob", func(t *testing.T) {

		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)

		h, errCh := runRouter(t, ctx)

		jobs := []string{"ns||app1||j1", "ns||app1||j2", "ns||app2||j3"}

		for _, name := range jobs {
			h.enqueue(informPutEvent(name))
		}

		for _, name := range jobs {
			h.enqueue(informDeleteEvent(name))
		}

		for _, name := range jobs {
			h.enqueue(closeJobEvent(name))
		}

		for _, name := range jobs {
			h.enqueue(closeJobEvent(name))
		}

		h.enqueue(informPutEvent("survivor"))

		waitClean(t, h, errCh)
	})
}
