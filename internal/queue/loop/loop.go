/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package loop

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/dapr/kit/events/loop"
	eventsqueue "github.com/dapr/kit/events/queue"
	"github.com/go-logr/logr"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/internal/api/queue"
	"github.com/diagridio/go-etcd-cron/internal/counter"
	"github.com/diagridio/go-etcd-cron/internal/queue/cache"
	"github.com/diagridio/go-etcd-cron/internal/queue/lineguide"
	"github.com/diagridio/go-etcd-cron/internal/queue/staging"
)

type Options struct {
	Log       logr.Logger
	TriggerFn api.TriggerFunction
	Queue     *eventsqueue.Processor[string, counter.Interface]
	LineGuide *lineguide.LineGuide
	Cache     *cache.Cache
	Staging   *staging.Staging
}

type inflight struct {
	doneCh chan struct{}
	idx    uint64
	cancel context.CancelFunc
}

type Loop struct {
	log       logr.Logger
	triggerFn api.TriggerFunction
	queue     *eventsqueue.Processor[string, counter.Interface]
	lineguide *lineguide.LineGuide
	cache     *cache.Cache
	staging   *staging.Staging

	loop *loop.Loop[*queue.ControlEvent]

	inflights     map[string]*inflight
	closing       atomic.Bool
	infligtsEmpty chan struct{}

	idx uint64
}

func New(opts Options) *Loop {
	l := &Loop{
		inflights:     make(map[string]*inflight),
		log:           opts.Log.WithName("loop"),
		triggerFn:     opts.TriggerFn,
		queue:         opts.Queue,
		lineguide:     opts.LineGuide,
		cache:         opts.Cache,
		staging:       opts.Staging,
		infligtsEmpty: make(chan struct{}),
	}

	l.loop = loop.New(loop.Options[*queue.ControlEvent]{
		Handler: l.handle,
	})

	return l
}

func (l *Loop) Run(ctx context.Context) error {
	pctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	go func() {
		errCh <- l.loop.Run(pctx)
	}()

	<-ctx.Done()

	l.loop.Enqueue(&queue.ControlEvent{
		Action: new(queue.ControlEvent_Close),
	})

	select {
	case <-l.infligtsEmpty:
	case <-time.After(time.Millisecond * 100):
	}

	cancel()

	return <-errCh
}

func (l *Loop) Enqueue(event *queue.ControlEvent) {
	l.loop.Enqueue(event)
}

func (l *Loop) handle(ctx context.Context, event *queue.ControlEvent) error {
	defer func() {
		if l.closing.Load() && len(l.inflights) == 0 {
			select {
			case <-l.infligtsEmpty:
			default:
				close(l.infligtsEmpty)
			}
		}
	}()

	switch action := event.GetAction().(type) {
	case *queue.ControlEvent_Informed:
		return l.handleInformed(ctx, action.Informed)

	case *queue.ControlEvent_ExecuteRequest:
		return l.handleExecuteRequest(ctx, action.ExecuteRequest)

	case *queue.ControlEvent_ExecuteResponse:
		l.handleExecuteResponse(ctx, action.ExecuteResponse)
		return nil

	case *queue.ControlEvent_DeliverablePrefixes:
		l.staging.DeliverablePrefixes(action.DeliverablePrefixes.GetPrefixes()...)
		return nil

	case *queue.ControlEvent_UndeliverablePrefixes:
		l.staging.UnDeliverablePrefixes(action.UndeliverablePrefixes.GetPrefixes()...)
		return nil

	case *queue.ControlEvent_Close:
		l.closing.Store(true)
		return nil

	default:
		return errors.New("unknown action type")
	}
}

func (l *Loop) handleInformed(ctx context.Context, action *queue.Informed) error {
	jobName := action.GetName()

	if inflight, ok := l.inflights[jobName]; ok {
		inflight.cancel()
		delete(l.inflights, jobName)
	}

	l.staging.Unstage(jobName)

	if action.GetIsPut() {
		return l.lineguide.Schedule(ctx, jobName, action.GetJobModRevision(), action.GetJob())
	}

	l.lineguide.Deschedule(jobName, action.GetJob())

	return nil
}

func (l *Loop) handleExecuteRequest(ctx context.Context, action *queue.ExecuteRequest) error {
	counter, ok := l.cache.Load(action.GetJobName())
	if !ok {
		return nil
	}

	idx := l.idx
	l.idx++

	ctx, cancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})

	l.inflights[action.GetJobName()] = &inflight{
		idx:    idx,
		cancel: func() { cancel(); <-doneCh },
		doneCh: doneCh,
	}

	go func() {
		defer func() {
			cancel()
			close(doneCh)
		}()

		result := l.triggerFn(ctx, counter.TriggerRequest())

		if ctx.Err() != nil {
			return
		}

		l.loop.Enqueue(&queue.ControlEvent{
			Action: &queue.ControlEvent_ExecuteResponse{
				ExecuteResponse: &queue.ExecuteResponse{
					JobName:    action.GetJobName(),
					CounterKey: action.GetCounterKey(),
					Uid:        idx,
					Result:     result,
				},
			},
		})
	}()

	return nil
}

func (l *Loop) handleExecuteResponse(ctx context.Context, action *queue.ExecuteResponse) {
	jobName := action.GetJobName()

	inflight, ok := l.inflights[jobName]
	if !ok || inflight.idx != action.GetUid() {
		return
	}

	defer delete(l.inflights, jobName)

	counter, ok := l.cache.Load(jobName)
	if !ok {
		return
	}

	if !l.handleTrigger(ctx, counter, action.GetResult().GetResult()) {
		l.cache.Delete(jobName)
	}
}

// handleTrigger handles triggering a schedule job.
// Returns true if the job is being re-enqueued, false otherwise.
func (l *Loop) handleTrigger(ctx context.Context, counter counter.Interface, result api.TriggerResponseResult) bool {
	switch result {
	// Job was successfully triggered. Re-enqueue if the Job has more triggers
	// according to the schedule.
	case api.TriggerResponseResult_SUCCESS:
		ok, err := counter.TriggerSuccess(ctx)
		if err != nil {
			l.log.Error(err, "failure marking job for next trigger", "name", counter.Key())
		}

		if ok {
			l.queue.Enqueue(counter)
		}

		return ok

		// The Job failed to trigger. Re-enqueue if the Job has more trigger
		// attempts according to FailurePolicy, or the Job has more triggers
		// according to the schedule.
	case api.TriggerResponseResult_FAILED:
		ok, err := counter.TriggerFailed(ctx)
		if err != nil {
			l.log.Error(err, "failure failing job for next retry trigger", "name", counter.Key())
		}

		if ok {
			l.queue.Enqueue(counter)
		}

		return ok

		// The Job was undeliverable so will be moved to the staging queue where it
		// will stay until it become deliverable. Due to a race, if the job is in
		// fact now deliverable, we need to re-enqueue immediately, else simply
		// keep it in staging until the prefix is deliverable.
	case api.TriggerResponseResult_UNDELIVERABLE:
		if !l.staging.Stage(counter) {
			l.queue.Enqueue(counter)
		}
		return true

	default:
		l.log.Error(errors.New("unknown trigger response result"), "unknown trigger response result", "name", counter.Key(), "result", result)
		return false
	}
}
