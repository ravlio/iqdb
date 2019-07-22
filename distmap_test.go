package iqdb_test

import (
	"github.com/ravlio/iqdb"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

var m map[string]*iqdb.KV
var dm1 = iqdb.NewDistmap(1)
var dm10 = iqdb.NewDistmap(10)
var dm100 = iqdb.NewDistmap(100)

var rnd []string

const it = 1000000

func init() {
	m = make(map[string]*iqdb.KV)
	rnd = make([]string, 0, it)

	for i := 0; i < it; i++ {
		is := strconv.Itoa(i)
		// Generating predefined pseudo-random keys
		// To not impact rand calling on each bench iteration
		rnd = append(rnd, strconv.Itoa(rand.Intn(it)))

		kv := &iqdb.KV{Value: is}
		m[is] = kv
		_ = dm10.Set(is, kv)
		_ = dm100.Set(is, kv)
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

func Benchmark1ShardMapSequentialRead(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = dm1.Get(strconv.Itoa(i))
			i++
			if i > it {
				i = 0
			}
		}
	})
}

// The oblivious winner
func Benchmark1ShardMapRandomRead(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var i = 0
		for pb.Next() {
			_, _ = dm1.Get(rnd[i])
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
			_, _ = dm10.Get(strconv.Itoa(i))
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
			_, _ = dm10.Get(rnd[i])
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
			_, _ = dm100.Get(strconv.Itoa(i))
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
			_, _ = dm100.Get(rnd[i])
			i++
			if i >= it {
				i = 0
			}
		}
	})
}
