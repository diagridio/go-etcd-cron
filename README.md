# Go Etcd Cron

This package implements a distributed and fault tolerant cron scheduler, using etcd as a backend.
It is designed to be used in a distributed environment (such as Kubernetes), where multiple instances of the same process can run concurrently acting on a shared set of cron jobs.
Cron can be dynamically scaled up or down, and job execution will be balanced across the instances.

Jobs are scheduled on a distributed queue, where only one of the instances will trigger the job.
This ensures that the job is executed only (at least*) once.
Multiple instances share the load of triggering jobs.

Jobs can be "one shot" or recurring.
Recurring jobs can have a TTL, a delayed start, expiry time, and a maximum number of runs.

## Getting started

```go
import (
  "github.com/diagridio/go-etcd-cron/api"
  "github.com/diagridio/go-etcd-cron/cron"
)

func main() {
  cron, err := cron.New(cron.Options{
    Client:    client,
    Namespace: "abc",
    ID:        "helloworld",
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
  go cron.Run(context.Background())

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
}
```

## API

Cron supports `Add`, `Get`, and `Delete` operations which are indexed on the job name.

A Job itself is made up of the following fields:

- `Schedule`: A cron (repeated) expression that defines when the job should be triggered.
  Accepts a systemd cron like expression (`* 30 9 * * 1-5`) or an every expression (`@every 5m`). For more info see `./proto/job.proto`. Optional if `DueTime` is set.
- `DueTime`: A "point in time" string representing when the job schedule should start from, or the "one shot" time if other scheduling type fields are not provided.
  Accepts a "point in time" string in the format of `RFC3339`, Go duration string (therefore calculated from now), or non-repeating `ISO8601` duration.
  Optional if `Schedule` is set.
- `TTL`: Another "point in time" string representing when the job should expire.
  Must be greater than `DueTime` if both are set.
  Optional.
- `Repeats`: The number of times the job should be triggered. Must be greater than 0 if set.
  Optional.
- `Metadata`: A protobuf Any message that can be used to store any additional information about the job which will be passed to the trigger function.
  Optional.
- `Payload`: A protobuf Any message that can be used to store the main payload of the job which will be passed to the trigger function.
  Optional.
- `FailurePolicy` Controls whether the Job should be retired if the trigger
  function returns false. `Drop` doesn't retry the job, `Constant `Constant` will
  constantly retry the job trigger for a configurable interval, up to a configurable
  maximum number of retries (which could be infinite). By default, Jobs have a
  `Constant` policy, with a 1s interval and 3 maximum retries.

A job must have *at least* either a `Schedule` or a `DueTime` set.

### Undeliverable Jobs

It can be the case that a job trigger hasn't actually _failed_, but instead is simply undeliverable at the current time.
In such cases, the trigger function can return `UNDELIVERABLE` to indicate that the job should be moved to the "staging queue" to be held until it can be delivered.
Staged jobs can be marked as deliverable again by calling `cron.DeliverablePrefixes` with the prefixes of those job names.
Jobs whose name match these prefixes will be re-enqueued for delivery.

## Leadership

Leadership is fully dynamic, meaning that crons can be added or removed from a cluster at any time, and the running crons will self organize rebalancing the jobs between them.

The cron scheduler uses a partition key ownership model to ensure that only one partition instance of the scheduler is running at any given time.
At boot, the replica attempts to claim its partition key; a user provided string ID.
If it is successful, it will ensure that there are no other schedulers running with a different _partition total_ value.
Once a leadership event occurs (a cron is added or removed from the cluster), crons will dynamically rebalance the partitions between them.

Leadership keys are associated with an ETCD lease of 20s TTL to prevent stale leadership keys persisting forever in the event of an (unlikely) crash.

## Counter

An associated `counters` key is used to track the current state of a job that is scheduled.
It includes the last trigger time (if triggered), the number of times the job has been triggered, and the Partition ID of the associated job with the same name.
Counters are lazily deleted in bulk by a garbage collector that runs every 180s in an effort to reduce pressure of jobs triggering.

The scheduler will never miss triggering jobs.
If the scheduler falls behind in time (for example, due to downtime), it will catch up and trigger all jobs that were missed in immediate succession.
A single Job schedule will _only_ trigger one after the other, waiting for the response before scheduling the next.

## Testing

```bash
go test --race -v ./...
```

## History

This is a fork of [https://github.com/Scalingo/go-etcd-cron](https://github.com/Scalingo/go-etcd-cron), which had been based on [https://github.com/robfig/cron](https://github.com/robfig/cron).
