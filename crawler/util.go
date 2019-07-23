package crawler

import (
	"sync"
)

type MapSync struct {
	sync.RWMutex
	Values map[string]uint32
}

func newMapSync() *MapSync {
	return &MapSync {
		Values: make(map[string]uint32),
	}
}

func (m *MapSync) Get(key string) uint32 {
	m.RLock()
	defer m.RUnlock()
	return m.Values[key]
}

func (m *MapSync) Put(key string, value uint32) {
	m.Lock()
	defer m.Unlock()
	m.Values[key] = value
}

func (m *MapSync) Add(key string, value uint32) {
	temp := m.Get(key)
	m.Put(key, value+temp)
}
