package iqdb_test

import (
	"github.com/ravlio/iqdb"
	"strconv"
	"testing"
	"sync"
	"math/rand"
)

var m map[string]*iqdb.KV
var dm10 = iqdb.NewDistmap(10)
var dm100 = iqdb.NewDistmap(100)

var rnd []string

const it = 1000000

func init() {
	m = make(map[string]*iqdb.KV)
	rnd = make([]string, 0, it)

	for i := 0; i < it; i++ {
		is := strconv.Itoa(i)
		rnd = append(rnd, strconv.Itoa(rand.Intn(it)))

		kv := &iqdb.KV{Value: is}
		m[is] = kv
		dm10.Set(is, kv)
		dm100.Set(is, kv)
	}
}

func BenchmarkNormalMapSequentialRead(b *testing.B) {
	var i = 0
	mx := &sync.RWMutex{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mx.RLock()
			_ = m[strconv.Itoa(i)]
			mx.RUnlock()
			i++
			if i > it {
				i = 0
			}
		}
	})
}

func BenchmarkNormalMapRandomRead(b *testing.B) {
	mx := &sync.RWMutex{}
	b.RunParallel(func(pb *testing.PB) {
		var i = 0
		for pb.Next() {
			mx.RLock()
			_ = m[rnd[i]]
			mx.RUnlock()
			i++
			if i >= it {
				i = 0
			}
		}
	})
}

func Benchmark10ShardMapSequentialRead(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dm10.Get(strconv.Itoa(i))
			i++
			if i > it {
				i = 0
			}
		}
	})
}

func Benchmark10ShardMapRandomRead(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var i = 0
		for pb.Next() {
			dm10.Get(rnd[i])
			i++
			if i >= it {
				i = 0
			}
		}
	})
}

func Benchmark100ShardMapSequentialRead(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			dm100.Get(strconv.Itoa(i))
			i++
			if i > it {
				i = 0
			}
		}
	})
}

func Benchmark100ShardMapRandomRead(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var i = 0
		for pb.Next() {
			dm100.Get(rnd[i])
			i++
			if i >= it {
				i = 0
			}
		}
	})
}