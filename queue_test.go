package queue_test

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/enorith/queue"
	"github.com/enorith/queue/std"
)

type Payload struct {
	Foo string
}

func Test_NsqProducer(t *testing.T) {
	e := queue.DefaultDispatcher.On("nsq").Dispatch(Payload{
		Foo: "ba777r",
	})
	if e != nil {
		t.Fatal(e)
	}
}

func Test_NsqCumsumer(t *testing.T) {
	done := make(chan struct{}, 1)
	queue.DefaultManager.Work(done, "nsq")

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	<-exit
	done <- struct{}{}
}

func init() {
	queue.WithDefaults()
	c, _ := queue.DefaultManager.ResolveConnection("nsq", map[string]interface{}{
		"nsqd": "127.0.0.1:49157",
	})
	queue.DefaultManager.RegisterWorker("nsq", std.NewWorker(4, c))

	c2, _ := queue.DefaultManager.ResolveConnection("mem", nil)

	queue.DefaultManager.RegisterWorker("mem", std.NewWorker(1, c2))

	std.AddHandler(Payload{}, func(p Payload) {
		fmt.Printf("handle job: payload %s \n", p.Foo)
	})
}
