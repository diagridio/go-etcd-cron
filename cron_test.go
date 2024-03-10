/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
	cron.Start(context.Background())

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-stop(cron):
	}
}

// Start, stop, then add an entry. Verify entry doesn't run.
func TestStopCausesJobsToNotRun(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.Start(context.Background())
	cron.Stop()
	cron.AddJob(Job{
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
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-add-before-running",
		Rhythm: "* * * * * ?",
	})
	cron.Start(context.Background())
	defer cron.Stop()

	// Give cron 2 seconds to run our job (which is always activated).
	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Start cron, add a job, expect it runs.
func TestAddWhileRunning(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.Start(context.Background())
	defer cron.Stop()

	cron.AddJob(Job{
		Name:   "test-run",
		Rhythm: "* * * * * ?",
	})

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test timing with Entries.
func TestSnapshotEntries(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-snapshot-entries",
		Rhythm: "@every 2s",
	})
	cron.Start(context.Background())
	defer cron.Stop()

	// Cron should fire in 2 seconds. After 1 second, call Entries.
	select {
	case <-time.After(ONE_SECOND):
		cron.Entries()
	}

	// Even though Entries was called, the cron should fire at the 2 second mark.
	select {
	case <-time.After(ONE_SECOND):
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
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			if s == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-multiple-1",
		Rhythm: "0 0 0 1 1 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-multiple-2",
		Rhythm: "* * * * * ?",
	})
	cron.AddJob(Job{
		Name:   "test-multiple-3",
		Rhythm: "0 0 0 31 12 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-multiple-4",
		Rhythm: "* * * * * ?",
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
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
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			if s == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-twice-1",
		Rhythm: "0 0 0 1 1 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-twice-2",
		Rhythm: "0 0 0 31 12 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-twice-3",
		Rhythm: "* * * * * ?",
	})

	cron.Start(context.Background())
	defer cron.Stop()

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
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			if s == "return-nil" {
				return nil
			}

			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}

	cron.AddJob(Job{
		Name:   "test-mschedule-1",
		Rhythm: "0 0 0 1 1 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-mschedule-2",
		Rhythm: "0 0 0 31 12 ?",
		Type:   "return-nil",
	})
	cron.AddJob(Job{
		Name:   "test-mschedule-3",
		Rhythm: "* * * * * ?",
	})
	cron.schedule(Every(time.Minute), Job{Name: "test-mschedule-4", Type: "return-nil"})
	cron.schedule(Every(time.Second), Job{Name: "test-mschedule-5"})
	cron.schedule(Every(time.Hour), Job{Name: "test-mschedule-6", Type: "return-nil"})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(2 * ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Test that the cron is run in the local time zone (as opposed to UTC).
func TestLocalTimezone(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	now := time.Now().Local()
	spec := fmt.Sprintf("%d %d %d %d %d ?",
		now.Second()+1, now.Minute(), now.Hour(), now.Day(), now.Month())

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-local",
		Rhythm: spec,
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
		t.FailNow()
	case <-wait(wg):
	}
}

// Simple test using Runnables.
func TestJob(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}

	cron.AddJob(Job{
		Name:   "job0",
		Rhythm: "0 0 0 30 Feb ?",
	})
	cron.AddJob(Job{
		Name:   "job1",
		Rhythm: "0 0 0 1 1 ?",
	})
	cron.AddJob(Job{
		Name:   "job2",
		Rhythm: "* * * * * ?",
	})
	cron.AddJob(Job{
		Name:   "job3",
		Rhythm: "1 0 0 1 1 ?",
	})
	cron.schedule(Every(5*time.Second+5*time.Nanosecond), Job{
		Name: "job4",
	})
	cron.schedule(Every(5*time.Minute), Job{
		Name: "job5",
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(ONE_SECOND):
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
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron1.Stop()

	cron2, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	defer cron2.Stop()

	job := Job{
		Name:   "test-parallel",
		Rhythm: "* * * * * ?",
	}
	cron1.AddJob(job)
	cron2.AddJob(job)

	cron1.Start(context.Background())
	cron2.Start(context.Background())

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
	firedOnce := false

	cron, err := New(
		WithNamespace(randomNamespace()),
		WithTriggerFunc(func(ctx context.Context, s string, p *anypb.Any) error {
			firedOnce = true
			wg.Done()
			return nil
		}))
	if err != nil {
		t.Fatal("unexpected error")
	}
	cron.AddJob(Job{
		Name:   "test-twice-3",
		Rhythm: "* * * * * ?",
		TTL:    2,
	})

	cron.Start(context.Background())
	defer cron.Stop()

	select {
	case <-time.After(6 * ONE_SECOND):
		// Success, it means it did not consume all the workgroup count because the job expired.
		assert.True(t, firedOnce)
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

func stop(cron *Cron) chan bool {
	ch := make(chan bool)
	go func() {
		cron.Stop()
		ch <- true
	}()
	return ch
}

func randomNamespace() string {
	return uuid.New().String()
}
