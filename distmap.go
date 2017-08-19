package iqdb

import (
	"sync"
	"crypto/sha1"
	"encoding/binary"
)

type distmap struct {
	shards     map[uint32]*shard
	mx         *sync.RWMutex
	shardCount uint32
}

type shard struct {
	kv map[string]*KV
	mx *sync.RWMutex
}

func (dm *distmap) getShard(key string) *shard {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	shardKey := binary.BigEndian.Uint32(hasher.Sum(nil)) % dm.shardCount

	dm.mx.Lock()
	defer dm.mx.Unlock()

	s, ok := dm.shards[shardKey]

	if ok {
		return s
	}

	s = &shard{
		kv: make(map[string]*KV),
		mx: &sync.RWMutex{},
	}
	dm.shards[shardKey] = s

	return s
}

func (dm *distmap) Get(key string) (*KV, error) {
	shard := dm.getShard(key)
	shard.mx.RLock()
	defer shard.mx.RUnlock()

	v, ok := shard.kv[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return v, nil
}

func (dm *distmap) Set(key string, kv *KV) error {
	shard := dm.getShard(key)
	shard.mx.Lock()
	defer shard.mx.Unlock()

	shard.kv[key] = kv

	return nil
}

func (dm *distmap) Range() chan<- string {
	out := make(chan<- string)

	shards := make([]uint32, len(dm.shards))

	dm.mx.RLock()
	var i = 0
	for s := range dm.shards {
		shards[i] = s
		i++
	}
	dm.mx.RUnlock()

	for _, sid := range shards {

		dm.mx.RLock()
		shard, ok := dm.shards[sid]
		dm.mx.RUnlock()

		if ok {
			go func() {
				shard.mx.RLock()
				for k := range shard.kv {
					out <- k
				}
				shard.mx.RUnlock()
			}()
		}
	}

	return out
}

func NewDistmap(shardCont uint32) *distmap {
	return &distmap{
		shardCount:shardCont,
		shards:make(map[uint32]*shard),
		mx:&sync.RWMutex{},
	}
}