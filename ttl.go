package iqdb

import (
	"time"
	"sync"
	"github.com/google/btree"
)

type TTLTreeItem interface {
	Less(btree.Item) bool
}

type ttlTreeItem struct {
	ttl time.Duration
	key string

	expire time.Time
}

func (i *ttlTreeItem) Less(than btree.Item) bool {
	if than == nil {
		return false
	}

	return i.expire.Before(than.(*ttlTreeItem).expire)
}

func NewttlTreeItem(key string, ttl time.Duration) ttlTreeItem {
	return ttlTreeItem{
		key:    key,
		ttl:    ttl,
		expire: timeFunc().Add(ttl),
	}
}

type ttlTree struct {
	delCb  func(key string) error
	tree   *btree.BTree
	ticker *time.Ticker

	mu *sync.Mutex
}

func (t *ttlTree) ReplaceOrInsert(item ttlTreeItem) {

	t.mu.Lock()
	t.tree.ReplaceOrInsert(&item)
	t.mu.Unlock()
}

func (t *ttlTree) Delete(item btree.Item) {
	t.mu.Lock()
	if t.tree.Has(item) {
		t.tree.Delete(item)
	}
	t.mu.Unlock()
}

func (t *ttlTree) loop() {
	for range t.ticker.C {
		t.checkTTL()
	}
}

func (t *ttlTree) checkTTL() {
	items := []btree.Item{}

	t.mu.Lock()
	t.tree.AscendLessThan(&ttlTreeItem{expire: timeFunc()}, func(item btree.Item) bool {
		items = append(items, item)

		return true
	})
	t.mu.Unlock()

	for _, item := range items {
		t.Delete(item)
		_ = t.delCb(item.(*ttlTreeItem).key)
	}
}

func NewTTLTree(delCb func(key string) error) *ttlTree {
	tree := &ttlTree{
		delCb:  delCb,
		tree:   btree.New(32),
		ticker: time.NewTicker(time.Second),

		mu: &sync.Mutex{},
	}
	go tree.loop()

	return tree
}
