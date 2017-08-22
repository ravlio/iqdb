package iqdb_test

import "testing"
import "github.com/ravlio/iqdb"
import (
	"os"
	"github.com/stretchr/testify/assert"
	"time"
)

var db *iqdb.IqDB

func TestMain(m *testing.M) {
	var err error
	db, err = iqdb.MakeServer(&iqdb.Options{TCPPort: 7777, HTTPPort: 8888, ShardCount: 100})

	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestOps(t *testing.T) {
	var err error

	ass := assert.New(t)

	t.Run("Standard KV", func(t *testing.T) {
		_, err = db.Get("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = db.Set("k", "")

		if !ass.NoError(err) {
			return
		}

		res, err := db.Get("k")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("", res) {
			return
		}

		err = db.Set("k", "val2")

		if !ass.NoError(err) {
			return
		}

		res, err = db.Get("k")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("val2", res) {
			return
		}

		err = db.Set("k2", "str1\n\rstr2")

		if !ass.NoError(err) {
			return
		}

		res, err = db.Get("k2")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal("str1\n\rstr2", res) {
			return
		}

		err = db.Remove("k2")

		if !ass.NoError(err) {
			return
		}

		res, err = db.Get("k2")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = db.Set("ttl", "val", time.Minute)

		if !ass.NoError(err) {
			return
		}

		res, err = db.Get("ttl")

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
		_, err := db.HashGetAll("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = db.HashGet("unexisting", "213")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = db.HashDel("unexisting", "123")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = db.HashKeys("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		err = db.HashSet("hash", "f1", "1")

		if !ass.NoError(err) {
			return
		}

		h, err := db.HashGetAll("hash")

		if !ass.Equal(map[string]string{"f1": "1"}, h) {
			return
		}

		v, err := db.HashGet("hash", "f1")

		if !ass.Equal(v, "1") {
			return
		}

		err = db.HashDel("hash", "unex")

		/*
		if !ass.Equal(iqdb.ErrHashKeyNotFound, err) {
			return
		}
		*/

		keys, err := db.HashKeys("hash")

		if !ass.Equal([]string{"f1"}, keys) {
			return
		}

		err = db.HashDel("hash", "f1")

		if !ass.NoError(err) {
			return
		}

		err = db.HashSet("hash", "k1")

		if !ass.Equal(iqdb.ErrHashKeyValueMismatch, err) {
			return
		}

		err = db.HashSet("hash", "k1", "v1", "k2", "v2")

		if !ass.NoError(err) {
			return
		}

		err = db.HashSet("hash", "k3", "v3")

		if !ass.NoError(err) {
			return
		}

		h, err = db.HashGetAll("hash")

		if !ass.Equal(map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"}, h) {
			return
		}

		err = db.Remove("hash")

		if !ass.NoError(err) {
			return
		}

		_, err = db.HashGetAll("hash")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}
	})

	if t.Failed() {
		return
	}

	t.Run("Lists", func(t *testing.T) {
		_, err := db.ListLen("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = db.ListIndex("unexisting", 1)

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = db.ListPop("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		_, err = db.ListRange("unexisting", 0, 10)

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		c, err := db.ListPush("list", "a", "b", "c", "d")

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues(c, 4) {
			return
		}

		c, err = db.ListPop("list")

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues(c, 3) {
			return
		}

		l, err := db.ListIndex("list", 1)

		if !ass.NoError(err) {
			return
		}

		if !ass.EqualValues(l, "b") {
			return
		}

		l, err = db.ListIndex("list", 10)

		if !ass.Equal(iqdb.ErrListIndexError, err) {
			return
		}

		_, err = db.ListRange("list", 0, 10)

		if !ass.Equal(iqdb.ErrListOutOfBounds, err) {
			return
		}

		lr, err := db.ListRange("list", 0, 2)

		if !ass.Equal([]string{"a", "b", "c"}, lr) {
			return
		}

	})

	t.Run("TTL", func(t *testing.T) {
		db.Set("nottl", "test1")
		db.Set("ttl1sec", "test2", time.Second*1)
		db.Set("ttl10sec", "test3", time.Second*10)

		v, err := db.Get("nottl")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test1") {
			return
		}

		v, err = db.Get("ttl1sec")

		if !ass.NoError(err) {
			return
		}
		if !ass.EqualValues(v, "test2") {
			return
		}

		v, err = db.Get("ttl10sec")

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
		_, err = db.Get("ttl1sec")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		v, err = db.Get("ttl10sec")

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

		_, err = db.Get("ttl10sec")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		db.TTL("nottl", time.Second)

		timeShift = timeShift + time.Second*2

		iqdb.SetTimeFunc(func() time.Time {
			return time.Now().Add(timeShift)
		})
		db.ForeTTLRecheck()

		_, err = db.Get("nottl")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

	})
	if t.Failed() {
		return
	}

}
