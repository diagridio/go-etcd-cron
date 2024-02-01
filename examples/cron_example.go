package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	etcdcron "github.com/diagridio/go-etcd-cron"
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

	log.Printf("starting hostId=%d for total of %d hosts and %d partitions", hostId, numHosts, numPartitions)

	p, err := etcdcron.NewPartitioning(numPartitions, numHosts, hostId)
	if err != nil {
		log.Fatal("fail to create partitioning", err)
	}
	cron, err := etcdcron.New(
		etcdcron.WithNamespace("example"),
		etcdcron.WithPartitioning(p))
	if err != nil {
		log.Fatal("fail to create etcd-cron", err)
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	var wg sync.WaitGroup
	// Start a goroutine to listen for signals
	go func() {
		// Wait for a signal
		sig := <-signalChannel
		fmt.Println("\nReceived signal:", sig)

		// Clean up and notify the main goroutine to exit
		cron.Stop()
		wg.Done()
	}()

	cron.AddJob(etcdcron.Job{
		Name:   "error-every-4s",
		Rhythm: "*/4 * * * * *",
		Func: func(ctx context.Context) error {
			// Use default logging of etcd-cron
			return errors.New("horrible error")
		},
	})
	cron.AddJob(etcdcron.Job{
		Name:   "echo-every-10s",
		Rhythm: "*/10 * * * * *",
		Func: func(ctx context.Context) error {
			log.Println("Every 10 seconds from", os.Getpid())
			return nil
		},
	})
	cron.Start(context.Background())

	// Wait for graceful shutdown on interrupt signal
	wg.Add(1)
	wg.Wait()

	fmt.Println("Program gracefully terminated.")
}
