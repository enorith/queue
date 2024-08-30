package connections

import (
	"fmt"
	"strings"
	"time"

	"github.com/enorith/queue/contracts"
	"github.com/enorith/queue/std"
	"github.com/nsqio/go-nsq"
)

const AddrStep = ","

type NsqConfig struct {
	Lookupd, Nsqd, Topic, Channel string
	UsingLookup, valid            bool
}

type Nsq struct {
	config    map[string]interface{}
	configVal NsqConfig
	consumer  *nsq.Consumer
	producer  *nsq.Producer
	handler   nsq.Handler
}

type WrapperHandler struct {
	Handler func(message *nsq.Message) error
}

func (wh WrapperHandler) HandleMessage(message *nsq.Message) error {
	return wh.Handler(message)
}
func (n *Nsq) Consume(concurrency int, exit chan struct{}, handler contracts.ErrorHandler) (err error) {
	c := nsq.NewConfig()
	config := n.configVal
	n.consumer, err = nsq.NewConsumer(config.Topic, config.Channel, c)
	if err != nil {
		return err
	}
	h := n.getHandler()
	ha := WrapperHandler{
		Handler: func(message *nsq.Message) error {
			return CallErrorHandler(h.HandleMessage(message), handler)
		},
	}

	if concurrency > 1 {
		n.consumer.AddConcurrentHandlers(ha, concurrency)
	} else {
		n.consumer.AddHandler(ha)
	}

	if config.UsingLookup {
		err = n.consumer.ConnectToNSQLookupds(strings.Split(config.Lookupd, AddrStep))
	} else {
		err = n.consumer.ConnectToNSQDs(strings.Split(config.Nsqd, AddrStep))
	}

	if err != nil {
		return
	}
	<-exit
	return
}

func (n *Nsq) getHandler() nsq.Handler {
	if n.handler == nil {
		n.handler = std.NsqHandler{}
	}
	return n.handler
}

func (n *Nsq) SetHandler(h nsq.Handler) *Nsq {
	n.handler = h
	return n
}

func (n *Nsq) Stop() error {
	if n.consumer != nil {
		n.consumer.Stop()
		n.consumer = nil
	}

	if n.producer != nil {
		n.producer.Stop()
		n.producer = nil
	}

	return nil
}

func (n *Nsq) Dispatch(payload interface{}, delay ...time.Duration) (err error) {
	c := nsq.NewConfig()
	config := n.configVal
	if n.producer == nil {
		n.producer, err = nsq.NewProducer(config.Nsqd, c)
		if err != nil {
			return
		}
	}

	n.producer.SetLogger(nil, 0)
	var messageBody []byte
	messageBody, err = std.MarshalPayload(payload)

	if err != nil {
		return
	}

	if len(delay) == 0 || delay[0] == 0 {
		err = n.producer.Publish(config.Topic, messageBody)
	} else {
		err = n.producer.DeferredPublish(config.Topic, delay[0], messageBody)
	}

	return
}

func (n *Nsq) parseConfig() (c NsqConfig, e error) {
	if n.configVal.valid {
		return n.configVal, nil
	}

	c.Topic = "default"
	c.Channel = "default"

	var is bool

	if lkupd, ok := n.config["nsqlookupd"]; ok {
		if c.Lookupd, is = lkupd.(string); !is {
			return c, fmt.Errorf("nsq config: nsqlookupd should be string, %T giving", lkupd)
		}
		c.UsingLookup = true
	}

	if nsqd, ok := n.config["nsqd"]; ok {
		if c.Nsqd, is = nsqd.(string); !is {
			return c, fmt.Errorf("nsq config: nsqd should be string, %T giving", nsqd)
		}
	}

	if topic, ok := n.config["topic"]; ok {
		if c.Topic, is = topic.(string); !is {
			return c, fmt.Errorf("nsq config: topic should be string, %T giving", topic)
		}
	}

	if channel, ok := n.config["channel"]; ok {
		if c.Channel, is = channel.(string); !is {
			return c, fmt.Errorf("nsq config: channel should be string, %T giving", channel)
		}
	}
	c.valid = true
	n.configVal = c

	return
}

func NewNsq(config map[string]interface{}) *Nsq {
	nsq := &Nsq{config: config}
	nsq.parseConfig()
	return nsq
}

func NewNsqFromConfig(conf NsqConfig) *Nsq {
	conf.valid = true
	if conf.Topic == "" {
		conf.Topic = "default"
	}

	if conf.Channel == "" {
		conf.Channel = "default"
	}
	if conf.Lookupd != "" {
		conf.UsingLookup = true
	}

	return &Nsq{
		configVal: conf,
	}
}
