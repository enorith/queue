package std

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/enorith/queue/contracts"
	"github.com/enorith/supports/reflection"
	"github.com/enorith/supports/str"
	"github.com/nsqio/go-nsq"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	handlers = make(map[string]interface{})
	mu       sync.RWMutex
)

var (
	messageType = reflect.TypeOf(&nsq.Message{})
)

var Invoker = func(payloadType reflect.Type, payloadValue, funcValue reflect.Value, funcType reflect.Type) error {

	var result []reflect.Value
	if payloadType.Kind() == reflect.Ptr {
		result = funcValue.Call([]reflect.Value{payloadValue})
	} else {
		result = funcValue.Call([]reflect.Value{reflect.Indirect(payloadValue)})
	}
	if len(result) > 0 {
		intf := result[0].Interface()
		if e, ok := intf.(error); ok {
			return e
		}
	}

	return nil
}

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
	ID          string
	PayloadType string
	Payload     []byte
}

func (j Job) Invoke(before ...func(payloadType reflect.Type, payloadValue reflect.Value)) (e error) {
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
	if t.NumIn() < 1 {
		return fmt.Errorf("[queue] job invoke handler should contain one param of payload")
	}
	pt := t.In(0)
	pv := reflect.New(pt)

	e = msgpack.Unmarshal(j.Payload, pv.Interface())
	if e != nil {
		return
	}

	for _, bf := range before {
		bf(pt, pv)
	}

	fv := reflect.ValueOf(fn)

	return Invoker(pt, pv, fv, t)
}

func ToJob(payload interface{}) (j Job, e error) {
	j.PayloadType = GetPayloadName(payload)
	j.ID = str.RandString(64)

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

type NsqHandler struct {
}

func (NsqHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return nil
	}
	var job Job

	e := msgpack.Unmarshal(m.Body, &job)
	if e != nil {
		log.Printf("[queue] parse job error %v", e)
		log.Print(e)
	}

	e = job.Invoke(func(pt reflect.Type, pv reflect.Value) {
		midx := reflection.SubStructOf(pt, messageType)
		if midx > -1 {
			reflect.Indirect(pv).Field(midx).Set(reflect.ValueOf(m))
		}
	})
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
	if t.NumIn() < 1 {
		return fmt.Errorf("[queue] job invoke handler should contain one param of payload")
	}
	ti := t.In(0)
	iv := reflect.New(ti)
	reflect.Indirect(iv).Set(reflect.ValueOf(payload))

	fv := reflect.ValueOf(fn)

	Invoker(ti, iv, fv, t)

	return nil
}
