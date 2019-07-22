package iqdb_test

import (
	"github.com/stretchr/testify/require"
	"testing"
)
import "github.com/ravlio/iqdb"
import (
	"os"
	"strconv"
	"time"
)

var db *iqdb.IqDB
var direct iqdb.Client
var redis iqdb.Client
var http iqdb.Client

func TestMain(m *testing.M) {
	var err error

	// Cleanup test db
	if _, err := os.Stat("test"); err == nil {
		os.Remove("test")
	}

	// Open new db or use existing one
	db, err = iqdb.Open("test", &iqdb.Options{RedisPort: 7777, HTTPPort: 8888, ShardCount: 100})

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

	redis, err = iqdb.NewRedisClient(":7777")
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

	req := require.New(t)

	// Open new db or use existing one
	aof, err := iqdb.Open("aof", &iqdb.Options{ShardCount: 100})

	if err != nil {
		panic(err)
	}

	err = aof.Set("k1", "v1", time.Second*10)
	req.NoError(err)
	req.NoError(aof.Set("k2", "v2"))
	req.NoError(aof.Set("k3", "v3"))
	req.NoError(aof.Set("k3", "v4"))
	req.NoError(aof.Remove("k2"))
	req.NoError(aof.HashSet("h1", "k1", "v1", "k2", "v2"))
	req.NoError(aof.Remove("h1"))
	req.NoError(aof.HashSet("h2", "k1", "v1", "k2", "v2"))
	req.NoError(aof.HashSet("h2", "k3", "v3"))
	req.NoError(aof.HashDel("h2", "k2"))
	_, err = aof.ListPush("l1", "a", "b", "c")
	req.NoError(err)

	req.NoError(aof.Remove("l1"))
	_, err = aof.ListPush("l1", "a", "b", "c")
	req.NoError(err)

	_, err = aof.ListPop("l1")
	req.NoError(err)

	// Closing DB

	req.NoError(aof.Close())

	aof, err = iqdb.Open("aof", &iqdb.Options{ShardCount: 100})

	req.NoError(err)

	v, err := aof.Get("k1")
	req.NoError(err)

	req.EqualValues("v1", v)

	v, err = aof.Get("k3")
	req.NoError(err)

	req.EqualValues("v4", v)

	_, err = aof.Get("k2")
	req.Equal(iqdb.ErrKeyNotFound, err)

	_, err = aof.HashKeys("h1")
	req.Equal(iqdb.ErrKeyNotFound, err)

	h, err := aof.HashGetAll("h2")

	req.NoError(err)

	req.Equal(map[string]string{"k1": "v1", "k3": "v3"}, h)

	l, err := aof.ListRange("l1", 0, 1)

	req.NoError(err)

	req.Equal([]string{"a", "b"}, l)

	err = aof.Close()
	req.NoError(err)
}

func testOps(t *testing.T, cl iqdb.Client) {
	var err error

	req := require.New(t)

	t.Run("Standard KV", func(t *testing.T) {
		_, err = cl.Get("unexisting")

		req.Equal(iqdb.ErrKeyNotFound, err)

		req.NoError(cl.Set("k", ""))

		res, err := cl.Get("k")
		req.NoError(err)

		req.Equal("", res)

		req.NoError(cl.Set("k", "val2"))

		res, err = cl.Get("k")

		req.NoError(err)

		req.Equal("val2", res)

		req.NoError(cl.Set("k2", "str1\n\rstr2"))

		res, err = cl.Get("k2")

		req.NoError(err)

		req.Equal("str1\n\rstr2", res)

		req.NoError(cl.Remove("k2"))

		res, err = cl.Get("k2")

		req.Equal(iqdb.ErrKeyNotFound, err)

		req.NoError(cl.Set("ttl", "val", time.Minute))

		res, err = cl.Get("ttl")

		req.NoError(err)

		req.Equal("val", res)
	})

	if t.Failed() {
		return
	}

	t.Run("Hashes", func(t *testing.T) {
		_, err := cl.HashGetAll("unexisting")

		req.Equal(iqdb.ErrKeyNotFound, err)

		_, err = cl.HashGet("unexisting", "213")

		req.Equal(iqdb.ErrKeyNotFound, err)

		err = cl.HashDel("unexisting", "123")

		req.Equal(iqdb.ErrKeyNotFound, err)

		_, err = cl.HashKeys("unexisting")

		req.Equal(iqdb.ErrKeyNotFound, err)

		req.NoError(cl.HashSet("hash", "f1", "1"))

		h, err := cl.HashGetAll("hash")

		req.Equal(map[string]string{"f1": "1"}, h)

		v, err := cl.HashGet("hash", "f1")

		req.Equal(v, "1")

		req.NoError(cl.HashDel("hash", "unex"))

		keys, err := cl.HashKeys("hash")

		req.Equal([]string{"f1"}, keys)

		req.NoError(cl.HashDel("hash", "f1"))

		err = cl.HashSet("hash", "k1")

		req.Equal(iqdb.ErrHashKeyValueMismatch, err)

		req.NoError(cl.HashSet("hash", "k1", "v1", "k2", "v2"))

		req.NoError(cl.HashSet("hash", "k3", "v3"))

		h, err = cl.HashGetAll("hash")

		req.Equal(map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}, h)

		req.NoError(cl.Remove("hash"))

		_, err = cl.HashGetAll("hash")

		req.Equal(iqdb.ErrKeyNotFound, err)
	})

	if t.Failed() {
		return
	}

	t.Run("Lists", func(t *testing.T) {
		_, err := cl.ListLen("unexisting")

		req.Equal(iqdb.ErrKeyNotFound, err)

		_, err = cl.ListIndex("unexisting", 1)

		req.Equal(iqdb.ErrKeyNotFound, err)

		_, err = cl.ListPop("unexisting")

		req.Equal(iqdb.ErrKeyNotFound, err)

		_, err = cl.ListRange("unexisting", 0, 10)

		req.Equal(iqdb.ErrKeyNotFound, err)

		c, err := cl.ListPush("list", "a", "b", "c", "d")

		req.NoError(err)

		req.EqualValues(4, c)

		c, err = cl.ListPop("list")

		req.NoError(err)

		req.EqualValues(3, c)

		l, err := cl.ListIndex("list", 1)

		req.NoError(err)

		req.EqualValues("b", l)

		l, err = cl.ListIndex("list", 10)

		req.Equal(iqdb.ErrListIndexError, err)

		_, err = cl.ListRange("list", 0, 10)

		req.Equal(iqdb.ErrListOutOfBounds, err)

		lr, err := cl.ListRange("list", 0, 2)

		req.Equal([]string{"a", "b", "c"}, lr)

	})

	t.Run("TTL", func(t *testing.T) {
		req.NoError(cl.Set("nottl", "test1"))
		req.NoError(cl.Set("ttl1sec", "test2", time.Second*1))
		req.NoError(cl.Set("ttl10sec", "test3", time.Second*10))

		v, err := cl.Get("nottl")

		require.NoError(t, err)

		req.EqualValues(v, "test1")

		v, err = cl.Get("ttl1sec")

		req.NoError(err)

		req.EqualValues(v, "test2")

		v, err = cl.Get("ttl10sec")

		req.NoError(err)

		req.EqualValues(v, "test3")

		timeShift := time.Second * 2

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})

		db.ForeTTLRecheck()
		_, err = cl.Get("ttl1sec")

		req.Equal(iqdb.ErrKeyNotFound, err)

		v, err = cl.Get("ttl10sec")

		req.NoError(err)

		req.EqualValues(v, "test3")

		timeShift = timeShift + time.Minute

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})

		db.ForeTTLRecheck()

		_, err = cl.Get("ttl10sec")

		req.Equal(iqdb.ErrKeyNotFound, err)

		req.NoError(cl.TTL("nottl", time.Second))

		timeShift = timeShift + time.Second*2

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})
		db.ForeTTLRecheck()

		_, err = cl.Get("nottl")

		req.Equal(iqdb.ErrKeyNotFound, err)

	})
	if t.Failed() {
		return
	}
}

func TestRedis(t *testing.T) {
	var err error

	req := require.New(t)

	req.NoError(redis.Set("k", "v", time.Second))

	k, err := redis.Get("k")
	req.NoError(err)

	req.Equal("v", k)
}

func TestOps(t *testing.T) {
	testOps(t, redis)

	if t.Failed() {
		return
	}
}

func Benchmark1Set(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = direct.Set(strconv.Itoa(i), strconv.Itoa(i), time.Second)
			i++
		}
	})
}

func Benchmark1Get(b *testing.B) {
	var i = 0
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = db.Get(strconv.Itoa(i))
			i++
		}
	})
}

func BenchmarkTCP1Set(b *testing.B) {
	var i = 0
	for n := 0; n < b.N; n++ {
		_ = redis.Set(strconv.Itoa(i), strconv.Itoa(i), time.Second)
		i++

	}
}

func BenchmarkTCP1Get(b *testing.B) {
	var i = 0

	for n := 0; n < b.N; n++ {
		_, _ = redis.Get(strconv.Itoa(i))
		i++
	}
}
