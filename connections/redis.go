package connections

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/enorith/queue/std"
	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
)

type Redis struct {
	rdb   redis.UniversalClient
	queue string
}

func (r *Redis) Consume(concurrency int, exit chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < concurrency; i++ {
		go r.loop(ctx)
	}
	<-exit

	return nil
}

func (r *Redis) Stop() error {
	return nil
}

func (r *Redis) Dispatch(payload interface{}, delay ...time.Duration) error {
	messageBody, err := std.MarshalPayload(payload)
	if err != nil {
		return err
	}

	ms := time.Now().Add(delay[0]).UnixMilli()

	r.rdb.ZAdd(context.Background(), r.queue, &redis.Z{
		Score:  float64(ms),
		Member: messageBody,
	})

	return nil
}

func (r *Redis) loop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("[queue] redis consume loop break")
			return
		default:
			rdb := r.rdb
			values, err := rdb.ZRangeByScore(ctx, r.queue, &redis.ZRangeBy{
				Min:    "0",
				Max:    fmt.Sprint(time.Now().UnixMilli()),
				Offset: 0,
				Count:  1,
			}).Result()
			if err != nil {
				log.Println("[queue] redis consume error", err)
				time.Sleep(time.Second)
				continue
			}
			if len(values) == 0 {
				time.Sleep(time.Second)
				continue
			}

			value := values[0]
			num, err := rdb.ZRem(ctx, r.queue, value).Result()
			if err != redis.Nil && err != nil {
				log.Println("[queue] redis consume error", err)
				time.Sleep(time.Second)
				continue
			}

			if num == 1 {
				var job std.Job
				e := msgpack.Unmarshal([]byte(value), &job)
				if e != nil {
					log.Printf("[queue] parse job error %v", e)
					log.Print(e)
				} else {
					job.Invoke()
				}
			}
		}
	}
}

func NewRedis(rdb redis.UniversalClient, queue string) *Redis {
	return &Redis{
		rdb:   rdb,
		queue: queue,
	}
}
