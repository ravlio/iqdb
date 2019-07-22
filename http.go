package iqdb

import (
	"time"
)

// TODO implement HTTP
type http struct {
}

func (h *http) Get(key string) (string, error) {
	panic("implement me")
}

func (h *http) Set(key, value string, ttl ...time.Duration) error {
	panic("implement me")
}

func (h *http) Remove(key string) error {
	panic("implement me")
}

func (h *http) TTL(key string, ttl time.Duration) error {
	panic("implement me")
}

func (h *http) Keys() chan<- string {
	panic("implement me")
}

func (h *http) ListLen(key string) (int, error) {
	panic("implement me")
}

func (h *http) ListIndex(key string, index int) (string, error) {
	panic("implement me")
}

func (h *http) ListPush(key string, value ...string) (int, error) {
	panic("implement me")
}

func (h *http) ListPop(key string) (int, error) {
	panic("implement me")
}

func (h *http) ListRange(key string, from, to int) ([]string, error) {
	panic("implement me")
}

func (h *http) HashGet(key string, field string) (string, error) {
	panic("implement me")
}

func (h *http) HashGetAll(key string) (map[string]string, error) {
	panic("implement me")
}

func (h *http) HashKeys(key string) ([]string, error) {
	panic("implement me")
}

func (h *http) HashDel(key string, field string) error {
	panic("implement me")
}

func (h *http) HashSet(key string, args ...string) error {
	panic("implement me")
}
