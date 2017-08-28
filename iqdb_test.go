package iqdb_test

import "testing"
import "github.com/ravlio/iqdb"
import (
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"time"
)

var db *iqdb.IqDB
var direct iqdb.Client
var tcp iqdb.Client
var http iqdb.Client

func TestMain(m *testing.M) {
	var err error

	// Cleanup test db
	if _, err := os.Stat("test"); err == nil {
		os.Remove("test")
	}

	// Open new db or use existing one
	db, err = iqdb.Open("test", &iqdb.Options{TCPPort: 7777, HTTPPort: 8888, ShardCount: 100})

	if err != nil {
		panic(err)
	}

	// Start servers
	// Additional goroutine is needed to allow make servers and clients in same time
	go func() {
		panic(db.Start())
	}()

	if err != nil {
		panic(err)
	}

	// Give it a chance to start listen connections before client will call
	time.Sleep(time.Second)

	// conversion from struct to Client interface
	var i interface{} = db

	direct = i.(iqdb.Client)

	tcp, err = iqdb.MakeTCPClient(":7777")
	if err != nil {
		panic(err)
	}

	/*http, err = iqdb.MakeHTTPClient(":8888")
	if err != nil {
		panic(err)
	}*/

	c := m.Run()

	err = db.Close()

	if err != nil {
		panic(err)
	}

	if _, err := os.Stat("test"); err == nil {
		os.Remove("test")
	} else {
		panic("no db file!")
	}

	os.Exit(c)
}

func TestAOF(t *testing.T) {
	var err error

	ass := assert.New(t)

	err = db.Set("k1", "v1", time.Second*10)
	if !ass.NoError(err) {
		return
	}
	err = db.Set("k2", "v2")
	if !ass.NoError(err) {
		return
	}
	err = db.Set("k3", "v3")
	if !ass.NoError(err) {
		return
	}

	err = db.Set("k3", "v4")
	if !ass.NoError(err) {
		return
	}
	err = db.Remove("k2")
	if !ass.NoError(err) {
		return
	}
	err = db.HashSet("h1", "k1", "v1", "k2", "v2")
	if !ass.NoError(err) {
		return
	}
	err = db.Remove("h1")
	if !ass.NoError(err) {
		return
	}
	err = db.HashSet("h2", "k1", "v1", "k2", "v2")
	if !ass.NoError(err) {
		return
	}
	err = db.HashSet("h2", "k3", "v3")
	if !ass.NoError(err) {
		return
	}
	err = db.HashDel("h2", "k2")
	if !ass.NoError(err) {
		return
	}
	_, err = db.ListPush("l1", "a", "b", "c")
	if !ass.NoError(err) {
		return
	}
	err = db.Remove("l1")
	if !ass.NoError(err) {
		return
	}
	_, err = db.ListPush("l1", "a", "b", "c")
	if !ass.NoError(err) {
		return
	}
	_, err = db.ListPop("l1")
	if !ass.NoError(err) {
		return
	}

	// Closing DB

	err = db.Close()

	if !ass.NoError(err) {
		return
	}

	db, err = iqdb.Open("test", &iqdb.Options{TCPPort: 7777, HTTPPort: 8888, ShardCount: 100})

	if !ass.NoError(err) {
		return
	}

	v, err := db.Get("k1")
	if !ass.NoError(err) {
		return
	}

	if !ass.EqualValues("v1", v) {
		return
	}

	v, err = db.Get("k3")
	if !ass.NoError(err) {
		return
	}

	if !ass.EqualValues("v4", v) {
		return
	}

	_, err = db.Get("k2")
	if !ass.Equal(iqdb.ErrKeyNotFound, err) {
		return
	}

	_, err = db.HashKeys("h1")
	if !ass.Equal(iqdb.ErrKeyNotFound, err) {
		return
	}

	h, err := db.HashGetAll("h2")

	if !ass.NoError(err) {
		return
	}

	if !ass.Equal(map[string]string{"k1": "v1", "k3": "v3"}, h) {
		return
	}

	l, err := db.ListRange("l1", 0, 1)

	if !ass.Equal([]string{"a", "b"}, l) {
		return
	}
}

func testOps(t *testing.T, cl iqdb.Client) {
	var err error

	ass := assert.New(t)

	t.Run("Standard KV", func(t *testing.T) {
		_, err = cl.Get("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = cl.Set("k", "")

		if !ass.NoError(err) {
			return
		}

		res, err := cl.Get("k")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("", res) {
			return
		}

		err = cl.Set("k", "val2")

		if !ass.NoError(err) {
			return
		}

		res, err = cl.Get("k")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("val2", res) {
			return
		}

		err = cl.Set("k2", "str1\n\rstr2")

		if !ass.NoError(err) {
			return
		}

		res, err = cl.Get("k2")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("str1\n\rstr2", res) {
			return
		}

		err = cl.Remove("k2")

		if !ass.NoError(err) {
			return
		}

		res, err = cl.Get("k2")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = cl.Set("ttl", "val", time.Minute)

		if !ass.NoError(err) {
			return
		}

		res, err = cl.Get("ttl")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("val", res) {
			return
		}
	})

	if t.Failed() {
		return
	}

	t.Run("Hashes", func(t *testing.T) {
		_, err := cl.HashGetAll("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = cl.HashGet("unexisting", "213")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = cl.HashDel("unexisting", "123")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = cl.HashKeys("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = cl.HashSet("hash", "f1", "1")

		if !ass.NoError(err) {
			return
		}

		h, err := cl.HashGetAll("hash")

		if !ass.Equal(map[string]string{"f1": "1"}, h) {
			return
		}

		v, err := cl.HashGet("hash", "f1")

		if !ass.Equal(v, "1") {
			return
		}

		err = cl.HashDel("hash", "unex")

		/*
			if !ass.Equal(iqdb.ErrHashKeyNotFound, err) {
				return
			}
		*/

		keys, err := cl.HashKeys("hash")

		if !ass.Equal([]string{"f1"}, keys) {
			return
		}

		err = cl.HashDel("hash", "f1")

		if !ass.NoError(err) {
			return
		}

		err = cl.HashSet("hash", "k1")

		if !ass.Equal(iqdb.ErrHashKeyValueMismatch, err) {
			return
		}

		err = cl.HashSet("hash", "k1", "v1", "k2", "v2")

		if !ass.NoError(err) {
			return
		}

		err = cl.HashSet("hash", "k3", "v3")

		if !ass.NoError(err) {
			return
		}

		h, err = cl.HashGetAll("hash")

		if !ass.Equal(map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}, h) {
			return
		}

		err = cl.Remove("hash")

		if !ass.NoError(err) {
			return
		}

		_, err = cl.HashGetAll("hash")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}
	})

	if t.Failed() {
		return
	}

	t.Run("Lists", func(t *testing.T) {
		_, err := cl.ListLen("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = cl.ListIndex("unexisting", 1)

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = cl.ListPop("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = cl.ListRange("unexisting", 0, 10)

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		c, err := cl.ListPush("list", "a", "b", "c", "d")

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues(4, c) {
			return
		}

		c, err = cl.ListPop("list")

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues(3, c) {
			return
		}

		l, err := cl.ListIndex("list", 1)

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues("b", l) {
			return
		}

		l, err = cl.ListIndex("list", 10)

		if !ass.Equal(iqdb.ErrListIndexError, err) {
			return
		}

		_, err = cl.ListRange("list", 0, 10)

		if !ass.Equal(iqdb.ErrListOutOfBounds, err) {
			return
		}

		lr, err := cl.ListRange("list", 0, 2)

		if !ass.Equal([]string{"a", "b", "c"}, lr) {
			return
		}

	})

	t.Run("TTL", func(t *testing.T) {
		cl.Set("nottl", "test1")
		cl.Set("ttl1sec", "test2", time.Second*1)
		cl.Set("ttl10sec", "test3", time.Second*10)

		v, err := cl.Get("nottl")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test1") {
			return
		}

		v, err = cl.Get("ttl1sec")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test2") {
			return
		}

		v, err = cl.Get("ttl10sec")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test3") {
			return
		}

		timeShift := time.Second * 2

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})

		db.ForeTTLRecheck()
		_, err = cl.Get("ttl1sec")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		v, err = cl.Get("ttl10sec")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test3") {
			return
		}

		timeShift = timeShift + time.Minute

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})

		db.ForeTTLRecheck()

		_, err = cl.Get("ttl10sec")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		cl.TTL("nottl", time.Second)

		timeShift = timeShift + time.Second*2

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})
		db.ForeTTLRecheck()

		_, err = cl.Get("nottl")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

	})
	if t.Failed() {
		return
	}
}

func TestTCP(t *testing.T) {
	var err error

	ass := assert.New(t)

	err = tcp.Set("k", "v", time.Second)
	if !ass.NoError(err) {
		return
	}

	k, err := tcp.Get("k")
	if !ass.NoError(err) {
		return
	}

	if !ass.Equal("v", k) {
		return
	}
}

func TestOps(t *testing.T) {
	testOps(t, tcp)

	if t.Failed() {
		return
	}
}

func Benchmark1Set(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			direct.Set(strconv.Itoa(i), strconv.Itoa(i), time.Second)
			i++
		}
	})
}

func Benchmark1Get(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Get(strconv.Itoa(i))
			i++
		}
	})
}

func BenchmarkTCP1Set(b *testing.B) {
	var i = 0
	for n := 0; n < b.N; n++ {
		tcp.Set(strconv.Itoa(i), strconv.Itoa(i), time.Second)
		i++

	}
}

func BenchmarkTCP1Get(b *testing.B) {
	var i = 0

	for n := 0; n < b.N; n++ {
		tcp.Get(strconv.Itoa(i))
		i++
	}
}
