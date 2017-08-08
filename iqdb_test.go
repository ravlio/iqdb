package iqdb_test

import "testing"
import "github.com/ravlio/iqdb"
import (
	"os"
	"github.com/stretchr/testify/assert"
	"time"
)

var db *iqdb.IqDB

func Main(m *testing.M) {
	var err error
	db, err = iqdb.MakeServer(&iqdb.Options{TCPPort: 7777, HTTPPort: 8888})

	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestOps(t *testing.T) {
	var err error

	ass := assert.New(t)

	t.Run("Standard KV", func(t *testing.T) {
		t.Log("unexisting key")
		_, err = db.Get("unexisting")

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		t.Log("empty key")
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

		t.Log("reassign existing key")
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

		t.Log("multistring key")
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

		t.Log("delete key")
		err = db.Remove("k2")

		if !ass.NoError(err) {
			return
		}

		res, err = db.Get("k2")

		if !ass.NoError(err) {
			return
		}

		if !ass.Equal(iqdb.ErrKeyNotFound, err) {
			return
		}

		t.Log("expiring key")
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

	})

	if t.Failed() {
		return
	}

}
