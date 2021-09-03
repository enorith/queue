package queue

import (
	"fmt"
	"log"
	"sync"

	"github.com/enorith/queue/connections"
	"github.com/enorith/queue/contracts"
)

type ConnectionRegister func(config map[string]interface{}) contracts.Connection

var DefaultManager = NewManager()

type Manager struct {
	connectionRegisters map[string]ConnectionRegister
	connections         map[string]contracts.Connection
	workers             map[string]contracts.Worker
	m                   sync.RWMutex
}

func (m *Manager) Work(done chan struct{}, workers ...string) {
	lenWorkers := len(workers)
	close := make(chan struct{}, lenWorkers)
	wg := new(sync.WaitGroup)

	for _, w := range workers {
		if worker, ok := m.GetWorker(w); ok {
			wg.Add(1)
			go func(worker contracts.Worker, w string) {
				defer wg.Done()
				log.Printf("[queue] worker [%s] listening", w)
				e := worker.Run(close)
				if e != nil {
					log.Printf("[queue] worker [%s] error: %v", w, e)
				} else {
					log.Printf("[queue] worker [%s] exited", w)
				}
			}(worker, w)
		}
	}
	go func() {
		<-done
		i := 0
		for i < lenWorkers {
			close <- struct{}{}
			i++
		}
	}()
	wg.Wait()
}

func (m *Manager) Close(workers ...string) {
	for _, w := range workers {
		if worker, ok := m.GetWorker(w); ok {
			log.Printf("[queue] worker [%s] closing", w)

			worker.Close()
		}
	}
}

func (m *Manager) ResolveConnection(connection string, config map[string]interface{}) (contracts.Connection, error) {
	m.m.RLock()
	if con, ok := m.connections[connection]; ok {
		m.m.RUnlock()
		return con, nil
	}

	cr, ok := m.connectionRegisters[connection]
	m.m.RUnlock()
	if !ok {
		return nil, fmt.Errorf("[queue] unregisterd connection [%s]", connection)
	}
	c := cr(config)
	m.m.Lock()
	m.connections[connection] = c
	m.m.Unlock()
	return c, nil
}

func (m *Manager) GetConnection(con string) (contracts.Connection, error) {
	m.m.RLock()
	defer m.m.RUnlock()
	if con, ok := m.connections[con]; ok {
		return con, nil
	}

	return nil, fmt.Errorf("[queue] unresolved connection [%s]", con)
}

func (m *Manager) GetWorker(worker string) (contracts.Worker, bool) {
	m.m.RLock()
	defer m.m.RUnlock()

	w, ok := m.workers[worker]

	return w, ok
}

func (m *Manager) RegisterWorker(name string, worker contracts.Worker) {
	m.m.Lock()
	defer m.m.Unlock()

	m.workers[name] = worker
}

func (m *Manager) RegisterConnection(connection string, cr ConnectionRegister) {
	m.m.Lock()
	defer m.m.Unlock()
	m.connectionRegisters[connection] = cr
}

func NewManager() *Manager {
	return &Manager{
		connectionRegisters: make(map[string]ConnectionRegister),
		workers:             make(map[string]contracts.Worker),
		connections:         make(map[string]contracts.Connection),
		m:                   sync.RWMutex{},
	}
}

func WithDefaults() {
	DefaultManager.RegisterConnection("nsq", func(config map[string]interface{}) contracts.Connection {
		return connections.NewNsq(config)
	})

	DefaultManager.RegisterConnection("mem", func(config map[string]interface{}) contracts.Connection {
		return connections.NewMem()
	})
}
