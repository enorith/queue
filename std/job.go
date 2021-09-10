package std

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/enorith/queue/contracts"
	"github.com/enorith/supports/reflection"
	"github.com/nsqio/go-nsq"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	handlers = make(map[string]interface{})
	mu       sync.RWMutex
)

func Listen(job interface{}, fn interface{}) {
	mu.Lock()
	defer mu.Unlock()
	name := reflection.TypeString(job)

	handlers[name] = fn
}

func GetHandler(name string) (interface{}, bool) {
	mu.RLock()
	defer mu.RUnlock()
	h, ok := handlers[name]
	return h, ok
}

type Job struct {
	PayloadType string
	Payload     []byte
}

func (j Job) Invoke() (e error) {
	defer func() {
		if x := recover(); x != nil {
			if err, ok := x.(error); ok {
				e = err
			}
			if s, ok := x.(string); ok {
				e = errors.New(s)
			}
		}
	}()
	fn, ok := GetHandler(j.PayloadType)
	if !ok {
		return fmt.Errorf("[queue] job handler %s not found", j.PayloadType)
	}

	t := reflection.TypeOf(fn)
	if t.Kind() != reflect.Func {
		return fmt.Errorf("[queue] job invoke only accept a func parameter, %T giving", fn)
	}
	if t.NumIn() != 1 {
		return fmt.Errorf("[queue] job invoke handler should contain one param of payload")
	}
	ti := t.In(0)
	iv := reflect.New(ti)

	msgpack.Unmarshal(j.Payload, iv.Interface())

	fv := reflect.ValueOf(fn)

	if ti.Kind() == reflect.Ptr {
		fv.Call([]reflect.Value{iv})
	} else {
		fv.Call([]reflect.Value{reflect.Indirect(iv)})
	}

	return nil
}

func ToJob(payload interface{}) (j Job, e error) {
	j.PayloadType = GetPayloadName(payload)

	j.Payload, e = msgpack.Marshal(payload)

	return
}

func MarshalPayload(payload interface{}) ([]byte, error) {
	j, e := ToJob(payload)

	if e != nil {
		return nil, e
	}

	return msgpack.Marshal(j)
}

type JobHandler struct {
}

func (JobHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return nil
	}
	var job Job

	msgpack.Unmarshal(m.Body, &job)
	e := job.Invoke()
	if e != nil {
		log.Printf("[queue] job %s handle error", job.PayloadType)
		log.Print(e)
	}

	return nil
}

func GetPayloadName(payload interface{}) string {
	if named, ok := payload.(contracts.NamedPayload); ok {
		return named.PayloadName()
	} else {
		return reflection.TypeString(payload)
	}
}

func InvokeHandler(payload interface{}) (e error) {

	defer func() {
		if x := recover(); x != nil {
			if err, ok := x.(error); ok {
				e = err
			}
			if s, ok := x.(string); ok {
				e = errors.New(s)
			}
		}
	}()
	name := GetPayloadName(payload)
	fn, ok := GetHandler(name)
	if !ok {
		return fmt.Errorf("[queue] job handler %s not found", name)
	}

	t := reflection.TypeOf(fn)
	if t.Kind() != reflect.Func {
		return fmt.Errorf("[queue] job invoke only accept a func parameter, %T giving", fn)
	}
	if t.NumIn() != 1 {
		return fmt.Errorf("[queue] job invoke handler should contain one param of payload")
	}
	ti := t.In(0)
	iv := reflect.New(ti)
	reflect.Indirect(iv).Set(reflect.ValueOf(payload))

	fv := reflect.ValueOf(fn)

	if ti.Kind() == reflect.Ptr {
		fv.Call([]reflect.Value{iv})
	} else {
		fv.Call([]reflect.Value{reflect.Indirect(iv)})
	}

	return nil
}
