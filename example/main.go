package main

import (
	"context"
	"net/url"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/cron"
)

func main() {
	cfg := embed.NewConfig()
	cfg.LogLevel = "error"
	cfg.Dir = "."
	lurl, err := url.Parse("http://127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	cfg.ListenPeerUrls = []url.URL{*lurl}
	cfg.ListenClientUrls = []url.URL{*lurl}

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		panic(err)
	}
	defer etcd.Close()

	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{etcd.Clients[0].Addr().String()},
	})
	if err != nil {
		panic(err)
	}

	cron, err := cron.New(cron.Options{
		Client:         client,
		Namespace:      "abc",
		PartitionID:    0,
		PartitionTotal: 1,
		TriggerFn: func(context.Context, *api.TriggerRequest) *api.TriggerResponse {
			// Do something with your trigger here.
			// Return SUCCESS if the trigger was successful, FAILED if the trigger
			// failed and should be subject to the FailurePolicy, or UNDELIVERABLE if
			// the job is currently undeliverable and should be moved to the staging
			// queue. Use `cron.DeliverablePrefixes` elsewhere to mark jobs with the
			// given prefixes as now deliverable.
			return &api.TriggerResponse{
				Result: api.TriggerResponseResult_SUCCESS,
				// Result: api.TriggerResponseResult_FAILED,
				// Result: api.TriggerResponseResult_UNDELIVERABLE,
			}
		},
	})
	if err != nil {
		panic(err)
	}

	// TODO: Pass proper context and do something with returned error.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	go func() {
		if err := cron.Run(ctx); err != nil {
			panic(err)
		}
	}()

	payload, _ := anypb.New(wrapperspb.String("hello"))
	meta, _ := anypb.New(wrapperspb.String("world"))
	tt := time.Now().Add(time.Second).Format(time.RFC3339)

	err = cron.Add(context.TODO(), "my-job", &api.Job{
		DueTime:  &tt,
		Payload:  payload,
		Metadata: meta,
	})
	if err != nil {
		panic(err)
	}

	cancel()
	time.Sleep(time.Second * 2)
}
