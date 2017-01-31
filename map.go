package concurrent_map

import (
	"hash/fnv"
	"sync"
)

// Concurrent map interface
type CMapInterface interface {
	Put(key string, value interface{})
	Get(key string) (interface{}, bool)
	IsExist(key string) bool
}

// A thread safe map
type CMap []*chunk

// A thread safe chunk of map
type chunk struct {
	data map[string]interface{}
	sync.RWMutex
}

// NewCMap creates a new concurrent map
func NewCMap(chunks uint32) CMapInterface {
	cmap := make(CMap, chunks)
	for i := uint32(0); i < chunks; i++ {
		cmap[i] = &chunk{data: map[string]interface{}{}}
	}
	return cmap
}

// getShard returns chunk for given key
func (t CMap) getShard(key string) *chunk {
	fnv := fnv.New32()
	fnv.Write([]byte(key))
	hash := fnv.Sum32() % uint32(len(t))
	return t[hash]
}

// Put sets given value for key
func (t CMap) Put(key string, value interface{}) {
	shard := t.getShard(key)
	shard.Lock()
	shard.data[key] = value
	shard.Unlock()
}

// Get returns value for givven key
func (t CMap) Get(key string) (interface{}, bool) {
	shard := t.getShard(key)
	shard.RLock()
	value, ok := shard.data[key]
	shard.RUnlock()
	return value, ok

}

// IsExist check for key present in map
func (t CMap) IsExist(key string) bool {
	shard := t.getShard(key)
	shard.RLock()
	_, ok := shard.data[key]
	shard.RUnlock()
	return ok
}
