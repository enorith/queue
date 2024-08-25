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
	"github.com/go-redis/redis/v8"
	"github.com/nsqio/go-nsq"
)

type Payload struct {
	*nsq.Message `msgpack:"-"`
	Foo          string
}

type MemPayload struct {
	Bar string
}

type RedisPayload struct {
	Message string
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

func Test_RedisProducer(t *testing.T) {
	for i := 0; i < 20; i++ {
		e := queue.DefaultDispatcher.On("redis").Dispatch(RedisPayload{
			Message: fmt.Sprintf("now %d", i),
		})
		if e != nil {
			t.Fatal(e)
		}

		e = queue.DefaultDispatcher.On("redis").After(time.Second * time.Duration(i)).Dispatch(RedisPayload{
			Message: fmt.Sprintf("delay %d", i),
		})

		if e != nil {
			t.Fatal(e)
		}
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

func Test_RedisCumsumer(t *testing.T) {
	done := make(chan struct{}, 1)
	queue.DefaultManager.Work(done, "redis")

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

	time.Sleep(time.Second)
	count := 20
	for i := 0; i < count; {
		bar := fmt.Sprintf("[%d] time %s", i, time.Now().Format(TF))
		queue.DefaultDispatcher.On("mem").After(time.Second).Dispatch(MemPayload{
			Bar: bar,
		})
		i++
	}
	fmt.Println("watting...")
	<-exit
	done <- struct{}{}
	wg.Wait()
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

	queue.DefaultManager.RegisterConnection("redis", func() (contracts.Connection, error) {
		return connections.NewRedis(redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs: []string{"127.0.0.1:6379"},
			DB:    2,
		}), "queue:default"), nil
	})

	c, _ := queue.DefaultManager.GetConnection("nsq")
	queue.DefaultManager.RegisterWorker("nsq", std.NewWorker(4, c))

	c2, _ := queue.DefaultManager.GetConnection("mem")

	queue.DefaultManager.RegisterWorker("mem", std.NewWorker(10, c2))

	c3, _ := queue.DefaultManager.GetConnection("redis")
	queue.DefaultManager.RegisterWorker("redis", std.NewWorker(3, c3))

	std.Listen(Payload{}, func(p Payload) {
		fmt.Println("nsq payload", p.ID)
		fmt.Printf("handle job: payload %s \n", p.Foo)
	})

	std.Listen(MemPayload{}, func(p MemPayload) {
		time.Sleep(time.Second)
		fmt.Printf("%s payload: %s \n", time.Now().Format(TF), p.Bar)
	})

	std.Listen(RedisPayload{}, func(r RedisPayload) {
		fmt.Printf("redis payload %s, %s \n", r.Message, time.Now().Format(TF))
	})
}
