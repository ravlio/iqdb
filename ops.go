package iqdb

import (
	"sync"
	"time"
)

type Client interface {
	Get(key string) (string, error)
	Set(key, value string, ttl ...time.Duration) error
	Remove(key string) error
	TTL(key string, ttl time.Duration) error
	Keys() chan<- string
	ListLen(key string) (int, error)
	ListIndex(key string, index int) (string, error)
	ListPush(key string, value ...string) (int, error)
	ListPop(key string) (int, error)
	ListRange(key string, from, to int) ([]string, error)
	HashGet(key string, field string) (string, error)
	HashGetAll(key string) (map[string]string, error)
	HashKeys(key string) ([]string, error)
	HashDel(key string, field string) error
	HashSet(key string, args ...string) error
}

// Get value by key
// Returns value in string on success and error on failure
func (iq *IqDB) Get(key string) (string, error) {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return "", err
	}

	if v.dataType != dataTypeKV {
		return "", ErrKeyTypeError
	}

	return v.Value, nil
}

// Set value by key. TTl is optional parameter
// Returns error on fail
func (iq *IqDB) Set(key, value string, ttl ...time.Duration) error {
	var t time.Duration

	if ttl != nil && ttl[0] > 0 {
		t = ttl[0]
	}

	err := iq.set(key, value, t, true)
	if err != nil {
		return err
	}

	err = iq.writeSet(key, value, t)

	return err
}

func (iq *IqDB) set(key, value string, ttl time.Duration, lock bool) error {
	kv := &KV{dataType: dataTypeKV, Value: value}

	if ttl > 0 {
		kv.ttl = ttl
	}

	err := iq.distmap.Set(key, kv)
	if err != nil {
		return err
	}

	if ttl > 0 {
		iq.ttl.ReplaceOrInsert(NewttlTreeItem(key, ttl))
	} else if iq.opts.TTL > 0 {
		iq.ttl.ReplaceOrInsert(NewttlTreeItem(key, iq.opts.TTL))
	}

	return nil
}

// Removes key from storage
// Returns error on fail
func (iq *IqDB) Remove(key string) error {
	err := iq.remove(key, true)

	err = iq.writeRemove(key)

	return err
}

func (iq *IqDB) remove(key string, lock bool) error {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return err
	}

	iq.distmap.Remove(key)

	if v.ttl > 0 {
		iq.ttl.Delete(&ttlTreeItem{ttl: v.ttl, key: key})
	}

	return nil
}
func (iq *IqDB) removeFromHash(key string) error {
	iq.distmap.Remove(key)

	return nil
}

// Set TTL on key
// Returns error on fail
func (iq *IqDB) TTL(key string, ttl time.Duration) error {
	err := iq._ttl(key, ttl, true)

	err = iq.writeTTL(key, ttl)

	return err
}

func (iq *IqDB) _ttl(key string, ttl time.Duration, lock bool) error {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return err
	}

	if v.ttl == ttl || ttl == 0 {
		return nil
	}

	v.ttl = ttl
	iq.ttl.ReplaceOrInsert(NewttlTreeItem(key, ttl))

	return nil
}

// Get all keys
// Returns string channel with keys
func (iq *IqDB) Keys() chan<- string {
	return iq.distmap.Range()
}

func (iq *IqDB) Type(key string) (int, error) {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return 0, err
	}

	return v.dataType, nil
}

// Lists

// Helper method to obtain and check data type
func (iq *IqDB) list(key string) (*list, error) {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return nil, err
	}

	if v.dataType != dataTypeList {
		return nil, ErrKeyTypeError
	}

	return v.list, nil
}

// Get list length
// Returns items count on success and error on fail
func (iq *IqDB) ListLen(key string) (int, error) {
	v, err := iq.list(key)

	if err != nil {
		return 0, err
	}

	return len(v.list), nil
}

// Get list item by its index
// Returns item on success and error on fail
func (iq *IqDB) ListIndex(key string, index int) (string, error) {
	v, err := iq.list(key)

	if err != nil {
		return "", err
	}

	if len(v.list) <= index {
		return "", ErrListIndexError
	}

	return v.list[index], nil
}

// Push item to end of list
// Returns items count on success and error on fail
func (iq *IqDB) ListPush(key string, value ...string) (int, error) {
	l, err := iq.listPush(key, value, true)

	err = iq.writeListPush(key, value...)

	return l, err
}

func (iq *IqDB) listPush(key string, value []string, lock bool) (int, error) {
	v, err := iq.list(key)

	if err != nil && err != ErrKeyNotFound {
		return 0, err
	} else if err == ErrKeyNotFound {
		l, err := iq.newList(key)

		if err != nil {
			return 0, err
		}
		v = l.list
	}

	v.mx.Lock()
	defer v.mx.Unlock()

	for _, val := range value {
		v.list = append(v.list, val)
	}

	return len(v.list), nil
}

// Pop item from end of list
// Returns items count on success and error on fail
func (iq *IqDB) ListPop(key string) (int, error) {
	l, err := iq.listPop(key, true)

	if err != nil {
		return 0, err
	}

	err = iq.writeListPop(key)

	return l, err
}

func (iq *IqDB) listPop(key string, lock bool) (int, error) {
	v, err := iq.list(key)

	if err != nil {
		return 0, err
	}

	v.mx.Lock()
	defer v.mx.Unlock()

	l := len(v.list)
	if l == 0 {
		return 0, nil
	}

	v.list = v.list[0 : l-1]

	return len(v.list), nil
}

// Get list items by key with specified index range
// Returns items slice on success and error on fail
func (iq *IqDB) ListRange(key string, from, to int) ([]string, error) {
	v, err := iq.list(key)

	if err != nil {
		return nil, err
	}

	if from < 0 || len(v.list) < to || from > to {
		return nil, ErrListOutOfBounds
	}

	return v.list[from : to+1], nil
}

// Hashes
func (iq *IqDB) hash(key string) (*hash, error) {
	v, err := iq.distmap.Get(key)

	if err != nil {
		return nil, err
	}

	if v.dataType != dataTypeHash {
		return nil, ErrKeyTypeError
	}

	return v.hash, nil
}

// Get hash value by key and field
// Returns value on success and error on fail
func (iq *IqDB) HashGet(key string, field string) (string, error) {
	v, err := iq.hash(key)

	if err != nil {
		return "", err
	}

	if s, ok := v.hash.Load(field); ok {
		return s.(string), nil
	}

	return "", ErrHashKeyNotFound
}

// Get hash fields and values map by key
// Returns map of fields and values on success and error on fail
func (iq *IqDB) HashGetAll(key string) (map[string]string, error) {
	v, err := iq.hash(key)

	if err != nil {
		return nil, err
	}

	ret := make(map[string]string)
	v.hash.Range(func(key, value interface{}) bool {
		ret[key.(string)] = value.(string)
		return true
	})

	return ret, nil
}

// Get hash keys of key
// Returns string slice of keys on success and error on fail
func (iq *IqDB) HashKeys(key string) ([]string, error) {
	v, err := iq.hash(key)

	if err != nil {
		return nil, err
	}

	ret := make([]string, 0)
	v.hash.Range(func(key, value interface{}) bool {
		ret = append(ret, key.(string))
		return true
	})

	return ret, nil
}

// Delete field from hash
// Returns error on fail
func (iq *IqDB) HashDel(key string, field string) error {
	err := iq.hashDel(key, field, true)
	if err != nil {
		return err
	}

	err = iq.writeHashDel(key, field)

	return err
}

func (iq *IqDB) hashDel(key, field string, lock bool) error {
	v, err := iq.hash(key)

	if err != nil {
		return err
	}

	// redundant call
	/*if _, ok := v.hash.Load(field); !ok {
		return ErrHashKeyNotFound
	}*/

	v.hash.Delete(field)

	return nil
}

// Set one or more field-value pairs on hash by key
// Example: HashSet("test","k1","v1","k2","v2")
// Returns error on fail
func (iq *IqDB) HashSet(key string, args ...string) error {

	if len(args)%2 != 0 {
		return ErrHashKeyValueMismatch
	}

	kv := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		kv[args[i]] = args[i+1]

	}

	err := iq.hashSet(key, kv, true)
	if err != nil {
		return err
	}

	err = iq.writeHashSet(key, args...)

	return err
}

func (iq *IqDB) hashSet(key string, kv map[string]string, lock bool) error {
	h, err := iq.hash(key)

	if err != nil && err != ErrKeyNotFound {
		return err
	} else if err == ErrKeyNotFound {
		nh, err := iq.newHash(key)

		if err != nil {
			return err
		}

		h = nh.hash
	}

	for k, v := range kv {
		h.hash.Store(k, v)
	}

	return nil
}

func (iq *IqDB) newHash(key string) (*KV, error) {
	kv := &KV{dataType: dataTypeHash, hash: &hash{&sync.Map{}}}
	err := iq.distmap.Set(key, kv)

	return kv, err
}

func (iq *IqDB) newList(key string) (*KV, error) {
	kv := &KV{dataType: dataTypeList, list: &list{mx: &sync.RWMutex{}, list: make([]string, 0)}}
	err := iq.distmap.Set(key, kv)

	return kv, err
}

func (iq *IqDB) ForeTTLRecheck() {
	iq.ttl.checkTTL()
}
