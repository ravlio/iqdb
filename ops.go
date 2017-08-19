package iqdb

import "time"

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
	db.kvmx.RLock()
	defer db.kvmx.RUnlock()
	v := db.kv[key]

	if v == nil {
		return "", ErrKeyNotFound
	}

	if v.dataType != dataTypeKV {
		return "", ErrKeyTypeError
	}

	return v.Value, nil
}

func (db *IqDB) Set(key, value string, ttl ...time.Duration) error {
	db.kvmx.Lock()
	defer db.kvmx.Unlock()

	kv := &KV{dataType: dataTypeKV, Value: value}
	if ttl[0] > 0 {
		kv.ttl = ttl[0]
	}

	db.kv[key] = kv

	if ttl[0] > 0 {
		db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: ttl[0], key: key})
	} else if db.opts.TTL > 0 {
		db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: db.opts.TTL, key: key})
	}

	return nil
}

func (db *IqDB) Remove(key string) error {
	db.kvmx.Lock()
	defer db.kvmx.Unlock()

	v := db.kv[key]

	if v == nil {
		return ErrKeyNotFound
	}

	delete(db.kv, key)

	if v.ttl > 0 {
		db.ttl.Delete(&ttlTreeItem{ttl: v.ttl, key: key})
	}

	return nil
}

func (db *IqDB) TTL(key string, ttl time.Duration) error {
	db.kvmx.Lock()
	defer db.kvmx.Unlock()
	v := db.kv[key]

	if v == nil {
		return ErrKeyNotFound
	}

	if v.ttl == ttl || ttl == 0 {
		return nil
	}

	v.ttl = ttl
	db.ttl.ReplaceOrInsert(ttlTreeItem{ttl: ttl, key: key})

	return nil
}

func (db *IqDB) Keys() ([]string, error) {
	resp := make([]string, len(db.kv))
	i := 0
	for k := range db.kv {
		db.kvmx.RLock()
		resp[i] = k
		db.kvmx.RUnlock()
	}

	return resp, nil
}

// Lists
/*
func (db *IqDB) ListLen(key string) (int, error) {

}

func (db *IqDB) ListIndex(key string, index int) (string, error) {

}

func (db *IqDB) ListPush(key string, value ...string) (int, error) {

}

func (db *IqDB) ListPop(key string) (int, error) {

}

func (db *IqDB) ListRange(key string, from, to int) ([]string, error) {

}

// Hashes
func (db *IqDB) HashLen(key string) (int, error) {

}

func (db *IqDB) HashGet(key string, field string) (string, error) {

}

func (db *IqDB) HashGetAll(key string) (map[string]string, error) {

}

func (db *IqDB) HashKeys(key string) ([]string, error) {

}

func (db *IqDB) HashDel(key string, field ...string) (int, error) {

}

func (db *IqDB) HashSet(key string, field int) (string, error) {

}
*/
