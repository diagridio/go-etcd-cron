/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/diagridio/go-etcd-cron/rhythm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

// Many tests schedule a job for every second, and then wait at most a second
// for it to run.  This amount is just slightly larger than 1 second to
// compensate for a few milliseconds of runtime.
const ONE_SECOND = 1*time.Second + 200*time.Millisecond

// Start and stop cron with no entries.
func TestNoEntries(t *testing.T) {
	cron, err := New(WithNamespace(randomNamespace()))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.Start(ctx)

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron, cancel):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.Start(ctx)
	cancel()
	cron.Wait()
	cron.AddJob(ctx, Job{
		Name:   "test-stop",
		Rhythm: "* * * * * ?",
	})

	select {
	case <-time.After(ONE_SECOND):
		// No job ran!
	case <-wait(wg):
		t.FailNow()
	}
}

// Add a job, start cron, expect it runs.
func TestAddBeforeRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	calledAlready := false
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if calledAlready {
				return nil
			}

			calledAlready = true
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:   "test-add-before-running",
		Rhythm: "* * * * * *",
	})
	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Add jobs with delayed start.
func TestDelayedStart(t *testing.T) {
	calledCount1 := atomic.Int32{}
	calledCount2 := atomic.Int32{}

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if m["id"] == "one" {
				calledCount1.Add(1)
			}
			if m["id"] == "two" {
				calledCount2.Add(1)
			}
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:      "test-delayed-start-1",
		Rhythm:    "* * * * * *",
		Metadata:  singleMetadata("id", "one"),
		StartTime: time.Now().Add(5 * time.Second),
	})
	cron.AddJob(ctx, Job{
		Name:      "test-delayed-start-2",
		Rhythm:    "@every 1s",
		Metadata:  singleMetadata("id", "two"),
		StartTime: time.Now().Add(5 * time.Second),
	})
	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	time.Sleep(4 * time.Second)
	assert.Equal(t, int32(0), calledCount1.Load())
	assert.Equal(t, int32(0), calledCount2.Load())

	time.Sleep(5 * time.Second)
	count1 := calledCount1.Load()
	count2 := calledCount2.Load()
	assert.True(t, (count1 == 4) || (count1 == 5), "count1 was: %d", count1)
	assert.True(t, (count2 == 4) || (count2 == 5), "count2 was: %d", count2)
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	cron.AddJob(ctx, Job{
		Name:   "test-run",
		Rhythm: "* * * * * ?",
	})

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:   "test-snapshot-entries",
		Rhythm: "@every 2s",
	})
	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	// After 1 second, call Entries.
	select {
	case <-time.After(ONE_SECOND):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire twice within 3 seconds (1 + 3).
	select {
	case <-time.After(3 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test delayed add after un starts for a while.
func TestDelayedAdd(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	called := false

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if m["op"] == "noop" {
				return nil
			}
			if called {
				t.Fatal("cannot call twice")
			}
			called = true
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:     "test-noop",
		Rhythm:   "@every 1s",
		Metadata: singleMetadata("op", "noop"),
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	// Artificial delay before add another record.
	time.Sleep(10 * time.Second)

	cron.AddJob(ctx, Job{
		Name:   "test-ev-2s",
		Rhythm: "@every 2s",
	})

	// Event should be called only once within 2 seconds.
	select {
	case <-time.After(3 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the entries are correctly sorted.
// Add a bunch of long-in-the-future entries, and an immediate entry, and ensure
// that the immediate entry runs immediately.
// Also: Test that multiple jobs run in the same instant.
func TestMultipleEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if m["op"] == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:     "test-multiple-1",
		Rhythm:   "0 0 0 1 1 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:   "test-multiple-2",
		Rhythm: "* * * * * ?",
	})
	cron.AddJob(ctx, Job{
		Name:     "test-multiple-3",
		Rhythm:   "0 0 0 31 12 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:   "test-multiple-4",
		Rhythm: "* * * * * ?",
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test running the same job twice.
func TestRunningJobTwice(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if m["op"] == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:     "test-twice-1",
		Rhythm:   "0 0 0 1 1 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:     "test-twice-2",
		Rhythm:   "0 0 0 31 12 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:   "test-twice-3",
		Rhythm: "* * * * * ?",
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

func TestRunningMultipleSchedules(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if m["op"] == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:     "test-mschedule-1",
		Rhythm:   "0 0 0 1 1 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:     "test-mschedule-2",
		Rhythm:   "0 0 0 31 12 ?",
		Metadata: singleMetadata("op", "return-nil"),
	})
	cron.AddJob(ctx, Job{
		Name:   "test-mschedule-3",
		Rhythm: "* * * * * ?",
	})
	cron.schedule(rhythm.Every(time.Minute), &Job{Name: "test-mschedule-4", Metadata: singleMetadata("op", "return-nil")})
	cron.schedule(rhythm.Every(time.Second), &Job{Name: "test-mschedule-5"})
	cron.schedule(rhythm.Every(time.Hour), &Job{Name: "test-mschedule-6", Metadata: singleMetadata("op", "return-nil")})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	called := atomic.Int32{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+2, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if called.Add(1) > 1 {
				return nil
			}
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:   "test-local",
		Rhythm: spec,
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(3 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	calledAlready := false
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			if calledAlready {
				return nil
			}
			calledAlready = true
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:   "job0",
		Rhythm: "0 0 0 30 Feb ?",
	})
	cron.AddJob(ctx, Job{
		Name:   "job1",
		Rhythm: "0 0 0 1 1 ?",
	})
	cron.AddJob(ctx, Job{
		Name:   "job2",
		Rhythm: "* * * * * ?",
	})
	cron.AddJob(ctx, Job{
		Name:   "job3",
		Rhythm: "1 0 0 1 1 ?",
	})
	cron.schedule(rhythm.Every(5*time.Second+5*time.Nanosecond), &Job{
		Name: "job4",
	})
	cron.schedule(rhythm.Every(5*time.Minute), &Job{
		Name: "job5",
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}

	// Ensure the entries are in the right order.
	expecteds := []string{"job2", "job4", "job5", "job1", "job3", "job0"}

	var actuals []string
	for _, entry := range cron.Entries() {
		actuals = append(actuals, entry.Job.Name)
	}

	for i, expected := range expecteds {
		if actuals[i] != expected {
			t.Errorf("Jobs not in the right order.  (expected) %s != %s (actual)", expecteds, actuals)
			t.FailNow()
		}
	}
}

// TestCron_Parallel tests that with 2 crons with the same job
// They should only execute once each job event
func TestCron_Parallel(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(2)

	cron1, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer func() {
		cancel1()
		cron1.Wait()
	}()

	cron2, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer func() {
		cancel2()
		cron2.Wait()
	}()

	job := Job{
		Name:   "test-parallel",
		Rhythm: "* * * * * ?",
	}
	cron1.AddJob(ctx1, job)
	cron2.AddJob(ctx2, job)

	cron1.Start(ctx1)
	cron2.Start(ctx2)

	select {
	case <-time.After(time.Duration(2) * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test job expires.
func TestTTL(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(5)

	firedOnce := atomic.Bool{}

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, m map[string]string, p *anypb.Any) error {
			firedOnce.Store(true)
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cron.AddJob(ctx, Job{
		Name:   "test-twice-3",
		Rhythm: "* * * * * ?",
		TTL:    2,
	})

	cron.Start(ctx)
	defer func() {
		cancel()
		cron.Wait()
	}()

	select {
	case <-time.After(6 * ONE_SECOND):
		// Success, it means it did not consume all the workgroup count because the job expired.
		assert.True(t, firedOnce.Load())
	case <-wait(wg):
		// Fails because TTL should delete the job and make it stop consuming the workgroup count.
		t.FailNow()
	}
}

func wait(wg *sync.WaitGroup) chan bool {
	ch := make(chan bool)
	go func() {
		wg.Wait()
		ch <- true
	}()
	return ch
}

func stop(cron *Cron, cancel context.CancelFunc) chan bool {
	ch := make(chan bool)
	go func() {
		cancel()
		cron.Wait()
		ch <- true
	}()
	return ch
}

func randomNamespace() string {
	return uuid.New().String()
}

func singleMetadata(key, value string) map[string]string {
	m := map[string]string{}
	m[key] = value
	return m
}
