package iqdb

import (
	"crypto/sha1"
	"encoding/binary"
	"sync"
)

type distmap struct {
	shards     []*shard
	mx         *sync.RWMutex
	shardCount int
}

type shard struct {
	kv *sync.Map
}

func (dm *distmap) getShard(key string) *shard {
	var shardKey int

	if dm.shardCount > 1 {
		hasher := sha1.New()
		hasher.Write([]byte(key))
		shardKey = int(binary.BigEndian.Uint32(hasher.Sum(nil)) % uint32(dm.shardCount))
	}

	return dm.shards[shardKey]
}

func (dm *distmap) Get(key string) (*KV, error) {
	shard := dm.getShard(key)

	v, ok := shard.kv.Load(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v.(*KV), nil
}

func (dm *distmap) Set(key string, kv *KV) error {
	shard := dm.getShard(key)

	shard.kv.Store(key, kv)
	return nil
}

func (dm *distmap) Remove(key string) error {
	shard := dm.getShard(key)

	_, ok := shard.kv.Load(key)
	if !ok {
		return ErrKeyNotFound
	}

	shard.kv.Delete(key)

	return nil
}

// Range through keys. Warning: maybe slow down other operations
// More shards - more speed
func (dm *distmap) Range() chan<- string {
	out := make(chan<- string)

	shards := make([]int, len(dm.shards))

	// Mutex is using only here
	dm.mx.RLock()
	var i = 0
	for s := range dm.shards {
		shards[i] = s
		i++
	}
	dm.mx.RUnlock()

	for _, sid := range shards {
		shard := dm.shards[sid]

		// Parallelize scanning between shards
		go func() {
			shard.kv.Range(func(key, value interface{}) bool {
				out <- key.(string)
				return true
			})

		}()
	}

	return out
}

func NewDistmap(shardCount int) *distmap {
	dm := &distmap{
		shardCount: shardCount,
		shards:     make([]*shard, shardCount),
		mx:         &sync.RWMutex{},
	}

	for i := 0; i < shardCount; i++ {
		dm.shards[i] = &shard{
			kv: &sync.Map{},
		}
	}

	return dm
}
