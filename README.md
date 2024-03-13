# Go Etcd Cron

Work-in-progress

[![GoDoc](http://godoc.org/github.com/diagridio/go-etcd-cron?status.png)](http://godoc.org/github.com/diagridio/go-etcd-cron)

## Goal

This package aims at implementing a distributed and fault tolerant cron in order to:

* Run an identical process on several hosts
* Each of these process instantiate a subset of the cron jobs
* Allow concurrent instances to execute the same job
* Ensure only one of these processes can trigger a job
* Number of cron jobs can scale by increasing the number of hosts
* Jobs are persisted and loaded from etcd
* Jobs can have a TTL and be auto-deleted
* Jobs can have a delayed start
* Jobs can have a max run count
* Cron can be used for multiple tenants, via namespacing

## Getting started

By default the library creates an etcd client on `127.0.0.1:2379`

```go
c, _ := etcdcron.NewEtcdMutexBuilder(clientv3.Config{
  Endpoints: []string{"etcd-host1:2379", "etcd-host2:2379"},
})
cron, _ := etcdcron.New(
  WithEtcdMutexBuilder(c),
  WithNamespace("my-example"),  // multi-tenancy
	WithTriggerFunc(func(ctx context.Context, triggerType string, payload *anypb.Any) error {
    log.Printf("Trigger from pid %d: %s %s\n", os.Getpid(), triggerType, string(payload.Value))
    return nil
	}),
  )
cron.AddJob(Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Type: "my-job-type",
	Payload: &anypb.Any{Value: []byte("hello every 2s")},
})
```

## Error Handling

```go
errorsHandler := func(ctx context.Context, job etcdcron.Job, err error) {
  // Do something with the error which happened during 'job'
}
etcdErrorsHandler := func(ctx context.Context, job etcdcron.Job, err error) {
  // Do something with the error which happened at the time of the execution of 'job'
  // But locking mecanism fails because of etcd error
}

cron, _ := etcdcron.New(
  WithErrorsHandler(errorsHandler),
  WithEtcdErrorsHandler(etcdErrorsHandler),
	WithTriggerFunc(func(ctx context.Context, triggerType string, payload *anypb.Any) error {
    log.Printf("Trigger from pid %d: %s %s\n", os.Getpid(), triggerType, string(payload.Value))
    return nil
	}),
)

cron.AddJob(context.TODO(), Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Type: "my-job-type",
	Payload: &anypb.Any{Value: []byte("hello every 2s")},
})
```

## Tests

Pre-requisites to run the tests locally:
- Run etcd locally via one of the options below:
  - Locally: [Install etcd](https://etcd.io/docs/v3.4/install/) then run `etcd --logger=zap`
  - Docker: [Running a single node etcd](https://etcd.io/docs/v3.5/op-guide/container/#running-a-single-node-etcd-1)

```bash
go test -v --race
```

## History

This is a fork of [https://github.com/Scalingo/go-etcd-cron](https://github.com/Scalingo/go-etcd-cron), which had been based on [https://github.com/robfig/cron](https://github.com/robfig/cron).

This fork has similar but still different goals from Scalingo's go-etcd-cron library.