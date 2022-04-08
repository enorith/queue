package queue_test

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/enorith/queue"
	"github.com/enorith/queue/connections"
	"github.com/enorith/queue/contracts"
	"github.com/enorith/queue/std"
)

type Payload struct {
	Foo string
}

type MemPayload struct {
	Bar string
}

var TF = "15:04:05"

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

func Test_Mem(t *testing.T) {
	done := make(chan struct{}, 1)
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		queue.DefaultManager.Work(done, "mem")
		wg.Done()
	}()
	ticker := time.NewTicker(time.Millisecond * 100)
	i := 0
	for {
		select {
		case <-ticker.C:
			queue.DefaultDispatcher.On("mem").Dispatch(MemPayload{
				Bar: fmt.Sprintf("[%d] time %s", i, time.Now().Format(TF)),
			})
			i++
		case <-exit:
			done <- struct{}{}
			return
		}
	}
}
func init() {
	queue.DefaultManager.RegisterConnection("nsq", func() (contracts.Connection, error) {
		return connections.NewNsqFromConfig(connections.NsqConfig{
			Nsqd: "127.0.0.1:4150",
		}), nil
	})
	queue.DefaultManager.RegisterConnection("mem", func() (contracts.Connection, error) {
		return connections.NewMem(), nil
	})

	c, _ := queue.DefaultManager.GetConnection("nsq")
	queue.DefaultManager.RegisterWorker("nsq", std.NewWorker(4, c))

	c2, _ := queue.DefaultManager.GetConnection("mem")

	queue.DefaultManager.RegisterWorker("mem", std.NewWorker(4, c2))

	std.Listen(Payload{}, func(p Payload) {
		fmt.Printf("handle job: payload %s \n", p.Foo)
	})

	std.Listen(MemPayload{}, func(p MemPayload) {
		time.Sleep(time.Second)
		fmt.Printf("%s payload: %s \n", time.Now().Format(TF), p.Bar)
	})
}
