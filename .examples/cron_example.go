/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	etcdcron "github.com/diagridio/go-etcd-cron"
	"github.com/diagridio/go-etcd-cron/partitioning"
	"google.golang.org/protobuf/types/known/anypb"
)

func main() {
	hostId, err := strconv.Atoi(os.Getenv("HOST_ID"))
	if err != nil {
		hostId = 0
	}
	numHosts, err := strconv.Atoi(os.Getenv("NUM_HOSTS"))
	if err != nil {
		numHosts = 1
	}
	numPartitions, err := strconv.Atoi(os.Getenv("NUM_PARTITIONS"))
	if err != nil {
		numPartitions = 1
	}
	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = "example"
	}

	log.Printf("starting hostId=%d for total of %d hosts and %d partitions", hostId, numHosts, numPartitions)

	p, err := partitioning.NewPartitioning(numPartitions, numHosts, hostId)
	if err != nil {
		log.Fatal("fail to create partitioning", err)
	}
	cron, err := etcdcron.New(
		etcdcron.WithNamespace(namespace),
		etcdcron.WithPartitioning(p),
		etcdcron.WithTriggerFunc(func(ctx context.Context, metadata map[string]string, payload *anypb.Any) (etcdcron.TriggerResult, error) {
			if metadata["failure"] == "yes" {
				// Failure does not trigger the errorsHandler() callback. It just skips the counter update.
				return etcdcron.Failure, nil
			}
			if metadata["stop"] == "random" {
				if rand.Int()%3 == 0 {
					return etcdcron.Delete, nil
				}
			}
			log.Printf("Trigger from pid %d: %s\n", os.Getpid(), string(payload.Value))
			return etcdcron.OK, nil
		}),
	)
	if err != nil {
		log.Fatal("fail to create etcd-cron", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	var wg sync.WaitGroup
	// Start a goroutine to listen for signals
	go func() {
		// Wait for a signal
		sig := <-signalChannel
		fmt.Println("\nReceived signal:", sig)

		// Clean up and notify the main goroutine to exit
		cancel()
		wg.Done()
	}()

	now := time.Now()
	if os.Getenv("ADD") == "1" {
		cron.AddJob(ctx, etcdcron.Job{
			Name:      "every-2s-dFG3F3DSGSGds",
			Rhythm:    "@every 2s",
			StartTime: now,
			Payload:   &anypb.Any{Value: []byte("ev 2s from now")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:      "every-2s-b34w5y5hbwthjs",
			Rhythm:    "@every 2s",
			StartTime: now.Add(time.Second), // odd seconds
			Payload:   &anypb.Any{Value: []byte("ev 2s from now+1s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:    "every-10s-bnsf45354wbdsnd",
			Rhythm:  "*/10 * * * * *",
			Payload: &anypb.Any{Value: []byte("ev 10s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:      "every-3s-mdhgm764324rqdg",
			Rhythm:    "@every 3s",
			StartTime: time.Now().Add(10 * time.Second),
			Payload:   &anypb.Any{Value: []byte("waits 10s then ev 3s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:    "every-4s-vdafbrtjnysh245",
			Rhythm:  "*/4 * * * * *",
			Repeats: 3, // Only triggers 3 times
			Payload: &anypb.Any{Value: []byte("ev 4s 3 times only")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:     "every-4s-nmdjfgx35u7jfsgjgsf",
			Rhythm:   "*/4 * * * * *",
			Repeats:  3, // Only triggers 3 times
			Metadata: map[string]string{"failure": "yes"},
			Payload:  &anypb.Any{Value: []byte("ev 4s never expires because it returns a failure condition")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:     "every-1s-agdg42y645ydfdha",
			Rhythm:   "@every 1s",
			Metadata: map[string]string{"stop": "random"},
			Payload:  &anypb.Any{Value: []byte("ev 1s with random stop")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:    "every-5s-adjbg43q5rbafbr44",
			Rhythm:  "*/5 * * * * *",
			Payload: &anypb.Any{Value: []byte("ev 5s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:    "every-6s-abadfh52jgdyj467",
			Rhythm:  "*/6 * * * * *",
			Payload: &anypb.Any{Value: []byte("ev 6s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:    "every-7s-bndasfbn4q55fgn",
			Rhythm:  "*/7 * * * * *",
			Payload: &anypb.Any{Value: []byte("ev 7s")},
		})
		cron.AddJob(ctx, etcdcron.Job{
			Name:       "every-1s-then-expire-hadfh452erhh",
			Rhythm:     "*/1 * * * * *",
			Expiration: time.Now().Add(10 * time.Second),
			Payload:    &anypb.Any{Value: []byte("ev 1s then expires after 10s")},
		})
	}
	cron.Start(ctx)

	// Wait for graceful shutdown on interrupt signal
	wg.Add(1)
	wg.Wait()

	fmt.Println("Program gracefully terminated.")
}
