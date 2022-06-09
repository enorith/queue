# Queue component of Enorith

## Usage

### Register connection

```golang
queue.DefaultManager.RegisterConnection("nsq", func() (contracts.Connection, error) {
    return connections.NewNsqFromConfig(connections.NsqConfig{
        Nsqd: "127.0.0.1:4150",
    }), nil
})
```

### Register worker

```golang
c, _ := queue.DefaultManager.GetConnection("nsq")
queue.DefaultManager.RegisterWorker("nsq", std.NewWorker(4, c))

```

### Run worker 

```golang 
done := make(chan struct{}, 1)
queue.DefaultManager.Work(done, "nsq")

exit := make(chan os.Signal, 1)
signal.Notify(exit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
<-exit
done <- struct{}{}
```