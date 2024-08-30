package connections

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/alitto/pond"
	"github.com/enorith/queue/contracts"
	"github.com/enorith/queue/std"
	"github.com/go-redis/redis/v8"
	"github.com/vmihailenco/msgpack/v5"
)

type Redis struct {
	rdb     redis.UniversalClient
	queue   string
	running bool
	pool    *pond.WorkerPool
}

func (r *Redis) Consume(concurrency int, exit chan struct{}, handler contracts.ErrorHandler) error {
	if !r.running {
		r.pool = pond.New(concurrency, DefaultMemBuffer)
		r.running = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.loop(ctx, int64(concurrency), handler)
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

func (r *Redis) loop(ctx context.Context, count int64, handler contracts.ErrorHandler) {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			log.Println("[queue] redis consume loop break")
			return
		case <-ticker.C:
			rdb := r.rdb
			values, err := rdb.ZRangeByScore(ctx, r.queue, &redis.ZRangeBy{
				Min:    "0",
				Max:    fmt.Sprint(time.Now().UnixMilli()),
				Offset: 0,
				Count:  count * 2,
			}).Result()
			if err != nil {
				log.Println("[queue] redis consume error", err)
			} else if len(values) > 0 {
				for _, value := range values {
					num, err := rdb.ZRem(ctx, r.queue, value).Result()
					if err != redis.Nil && err != nil {
						log.Println("[queue] redis consume error", err)
					} else if num == 1 {
						var job std.Job
						e := msgpack.Unmarshal([]byte(value), &job)
						if e != nil {
							log.Printf("[queue] parse job error %v", e)
							log.Print(e)
						} else {
							r.pool.Submit(func() {
								CallErrorHandler(job.Invoke(), handler)
							})
						}
					}
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
