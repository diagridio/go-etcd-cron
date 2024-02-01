# Go Etcd Cron

Work-in-progress

[![GoDoc](http://godoc.org/github.com/diagridio/go-etcd-cron?status.png)](http://godoc.org/github.com/diagridio/go-etcd-cron)

## Goal

This package aims at implementing a distributed and fault tolerant cron in order to:

* Run an identical process on several hosts
* Each of these process instantiate a subset of the cron jobs
* Ensure only one of these processes executes a job
* Number of cron jobs can scale by increasing the number of hosts
* Eventualy, a trigger can be missed due to change in leadership for a cron job

## Etcd Initialization

By default the library creates an etcd client on `127.0.0.1:2379`

```go
c, _ := etcdcron.NewEtcdMutexBuilder(clientv3.Config{
  Endpoints: []string{"etcd-host1:2379", "etcd-host2:2379"},
})
cron, _ := etcdcron.New(WithEtcdMutexBuilder(c))
cron.AddJob(Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Func: func(ctx context.Context) error {
    // Handler
  },
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
)

cron.AddJob(Job{
  Name: "job0",
  Rhythm: "*/2 * * * * *",
  Func: func(ctx context.Context) error {
    // Handler
  },
})
```

## History

This is a fork of [https://github.com/Scalingo/go-etcd-cron](https://github.com/Scalingo/go-etcd-cron), which had been based on [https://github.com/robfig/cron](https://github.com/robfig/cron).

This fork has similar but still different goals from Scalingo's go-etcd-cron library.