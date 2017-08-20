package iqdb

import (
	"time"
	"sync"
)

type Client interface {
	Get(key string) (string, error)
	Set(key string, ttl int) error
	Remove(key string) error
	TTL(key string, ttl int) error
	Keys() ([]string, error)
	ListLen(key string) (int, error)
	ListIndex(key string, index int) (string, error)
	ListPush(key string, value ...string) (int, error)
	ListPop(key string) (int, error)
	ListRange(key string, from, to int) ([]string, error)
	HashLen(key string) (int, error)
	HashGet(key string, field string) (string, error)
	HashGetAll(key string) (map[string]string, error)
	HashKeys(key string) ([]string, error)
	HashDel(key string, field ...string) (int, error)
	HashSet(key string, field int) (string, error)
}

// General KV
func (db *IqDB) Get(key string) (string, error) {
	v, err := db.distmap.Get(key)

	if err != nil {
		return "", err
	}

	if v.dataType != dataTypeKV {
		return "", ErrKeyTypeError
	}

	return v.Value, nil
}

// General set method. TTL may be optional as it's a slice
func (db *IqDB) Set(key, value string, ttl ...time.Duration) error {
	kv := &KV{dataType: dataTypeKV, Value: value}
	if ttl[0] > 0 {
		kv.ttl = ttl[0]
	}

	db.distmap.Set(key, kv)

	if ttl[0] > 0 {
		db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: ttl[0], key: key})
	} else if db.opts.TTL > 0 {
		db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: db.opts.TTL, key: key})
	}

	return nil
}

func (db *IqDB) Remove(key string) error {
	v, err := db.distmap.Get(key)

	if err != nil {
		return err
	}

	db.distmap.Remove(key)

	if v.ttl > 0 {
		db.ttl.Delete(&ttlTreeItem{ttl: v.ttl, key: key})
	}

	return nil
}

func (db *IqDB) TTL(key string, ttl time.Duration) error {
	v, err := db.distmap.Get(key)

	if err != nil {
		return err
	}

	if v.ttl == ttl || ttl == 0 {
		return nil
	}

	v.ttl = ttl
	db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: ttl, key: key})

	return nil
}

func (db *IqDB) Keys() chan<- string {
	return db.distmap.Range()
}

func (db *IqDB) Type(key string) (int, error) {
	v, err := db.distmap.Get(key)

	if err != nil {
		return 0, err
	}

	return v.dataType, nil
}

// Lists

// Helper method to obtain and check data type
func (db *IqDB) List(key string) (*list, error) {
	v, err := db.distmap.Get(key)

	if err != nil {
		return nil, err
	}

	if v.dataType != dataTypeList {
		return nil, ErrKeyTypeError
	}

	return v.list, nil
}

func (db *IqDB) ListLen(key string) (int, error) {
	v, err := db.List(key)

	if err != nil {
		return 0, err
	}

	return len(v.list), nil
}

func (db *IqDB) ListIndex(key string, index int) (string, error) {
	v, err := db.List(key)

	if err != nil {
		return "", err
	}

	if len(v.list) <= index {
		return "", ErrListIndexError
	}

	return v.list[index], nil
}

func (db *IqDB) ListPush(key string, value ...string) (int, error) {
	v, err := db.List(key)

	if err != nil && err != ErrKeyNotFound {
		return 0, err
	} else if err == ErrKeyNotFound {
		v.list = make([]string, 0)
	}

	v.mx.Lock()
	defer v.mx.Unlock()

	for _, val := range value {
		v.list = append(v.list, val)
	}

	return len(v.list), nil
}

func (db *IqDB) ListPop(key string) (int, error) {
	v, err := db.List(key)

	if err != nil {
		return 0, err
	}

	v.mx.Lock()
	defer v.mx.Unlock()

	l := len(v.list)
	if l == 0 {
		return 0, nil
	}

	v.list = v.list[0:l-1]

	return l, nil
}

func (db *IqDB) ListRange(key string, from, to int) ([]string, error) {
	v, err := db.List(key)

	if err != nil {
		return nil, err
	}

	if from < 0 || len(v.list) >= to || from > to {
		return nil, ErrListOutOfBounds
	}

	return v.list[from:to], nil
}

// Hashes
func (db *IqDB) Hash(key string) (*hash, error) {
	v, err := db.distmap.Get(key)

	if err != nil {
		return nil, err
	}

	if v.dataType != dataTypeHash {
		return nil, ErrKeyTypeError
	}

	return v.hash, nil
}

func (db *IqDB) HashGet(key string, field string) (string, error) {
	v, err := db.Hash(key)

	if err != nil {
		return nil, err
	}

	if s, ok := v.hash.Load(field); ok {
		return s.(string), nil
	}

	return "", ErrHashKeyNotFound
}

func (db *IqDB) HashGetAll(key string) (map[string]string, error) {
	v, err := db.Hash(key)

	if err != nil {
		return nil, err
	}

	ret := make(map[string]string)
	v.hash.Range(func(key, value interface{}) bool {
		ret[key.(string)] = value.(string)
	})

	return ret, nil
}

func (db *IqDB) HashKeys(key string) ([]string, error) {
	v, err := db.Hash(key)

	if err != nil {
		return nil, err
	}

	ret := make([]string, 0)
	v.hash.Range(func(key, value interface{}) bool {
		ret = append(ret, key.(string))
	})

	return ret, nil
}

func (db *IqDB) HashDel(key string, field string) error {
	v, err := db.Hash(key)

	if err != nil {
		return err
	}

	v.hash.Delete(field)

	return nil
}

func (db *IqDB) HashSet(key string, args ...string) error {

	if len(args)%2 != nil {
		return ErrHashKeyValueMismatch
	}

	v, err := db.Hash(key)

	if err != nil && err != ErrKeyNotFound {
		return err
	} else if err == ErrKeyNotFound {
		v.hash = &sync.Map{}
	}

	for i := 0; i < len(args); i += 2 {
		v.hash.Store(args[i], args[i+1])

	}

	return nil
}
