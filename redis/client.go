package redis

import (
	"errors"
	"github.com/ravlio/iqdb/redis"
	"strconv"
	"time"
	"github.com/ravlio/iqdb"
)

type client struct {
	r *redis.Reader
	w *redis.Writer
}

func getFirstBulkAsString(msg *redis.Message) (string, error) {
	if msg.Type != redis.TypeArray {
		return "", redis.ErrUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return "", redis.ErrUnknownParseError
	}

	return string(msg.Arr[0].Bulk), nil
}

func getFirstBulkAsStringSlice(msg *redis.Message) ([]string, error) {
	if msg.Type != redis.TypeArray {
		return nil, redis.ErrUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return nil, redis.ErrUnknownParseError
	}

	r := make([]string, len(msg.Arr))
	for i := range msg.Arr {
		r[i] = string(msg.Arr[i].Bulk)
	}

	return r, nil
}

func getFirstBulkAsStringMap(msg *redis.Message) (map[string]string, error) {
	if msg.Type != redis.TypeArray {
		return nil, redis.ErrUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return nil, redis.ErrUnknownParseError
	}

	r := make(map[string]string)
	for i := 0; i < len(msg.Arr); i += 2 {
		r[string(msg.Arr[i].Bulk)] = string(msg.Arr[i+1].Bulk)
	}

	return r, nil
}

func getFirstBulkAsInt(msg *redis.Message) (int, error) {
	if msg.Type != redis.TypeArray {
		return 0, redis.ErrUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return 0, redis.ErrUnknownParseError
	}

	return strconv.Atoi(string(msg.Arr[0].Bulk))
}

func checkErr(msg *redis.Message) error {
	if msg.Arr[0].Type == redis.TypeError {
		switch msg.Arr[0].Err.Error() {
		case iqdb.ErrKeyNotFound.Error():
			return iqdb.ErrKeyNotFound
		case iqdb.ErrKeyTypeError.Error():
			return iqdb.ErrKeyTypeError
		case iqdb.ErrListIndexError.Error():
			return iqdb.ErrListIndexError
		case iqdb.ErrListOutOfBounds.Error():
			return iqdb.ErrListOutOfBounds
		case iqdb.ErrHashKeyNotFound.Error():
			return iqdb.ErrHashKeyNotFound
		case iqdb.ErrHashKeyValueMismatch.Error():
			return iqdb.ErrHashKeyValueMismatch

		}
		return errors.New("Unknown error")
	}

	return nil
}

func (tcp *client) Get(key string) (string, error) {
	tcp.w.Write("GET", key)
	msg, err := tcp.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (tcp *client) Set(key, value string, ttl ...time.Duration) error {
	var err error
	if ttl != nil {
		err = tcp.w.Write("SET", key, value, ttl[0])
	} else {
		err = tcp.w.Write("SET", key, value)
	}

	if err != nil {
		return err
	}
	_, err = tcp.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (tcp *client) Remove(key string) error {
	err := tcp.w.Write("DEL", key)

	if err != nil {
		return err
	}

	_, err = tcp.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (tcp *client) TTL(key string, ttl time.Duration) error {
	err := tcp.w.Write("TTL", key, ttl)

	if err != nil {
		return err
	}

	_, err = tcp.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (tcp *client) Keys() chan<- string {
	panic("implement me")
}

func (tcp *client) ListLen(key string) (int, error) {
	err := tcp.w.Write("LLEN", key)

	if err != nil {
		return 0, err
	}

	m, err := tcp.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(m); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(m)
}

func (tcp *client) ListIndex(key string, index int) (string, error) {
	err := tcp.w.Write("LINDEX", key, index)

	if err != nil {
		return "", err
	}
	msg, err := tcp.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (tcp *client) ListPush(key string, value ...string) (int, error) {
	ss := make([]string, len(value)+2)
	ss[0] = "LPUSH"
	ss[1] = key

	for i, v := range value {
		ss[i+2] = v
	}
	err := tcp.w.WriteStringSlice(ss)
	if err != nil {
		return 0, err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(msg); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(msg)
}

func (tcp *client) ListPop(key string) (int, error) {
	err := tcp.w.Write("LPOP", key)

	if err != nil {
		return 0, err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(msg); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(msg)
}

func (tcp *client) ListRange(key string, from, to int) ([]string, error) {
	err := tcp.w.Write("LRANGE", key, from, to)

	if err != nil {
		return nil, err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringSlice(msg)
}

func (tcp *client) HashGet(key string, field string) (string, error) {
	err := tcp.w.Write("HGET", key, field)
	if err != nil {
		return "", err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (tcp *client) HashGetAll(key string) (map[string]string, error) {
	err := tcp.w.Write("HGETALL", key)
	if err != nil {
		return nil, err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringMap(msg)
}

func (tcp *client) HashKeys(key string) ([]string, error) {
	err := tcp.w.Write("HKEYS", key)
	if err != nil {
		return nil, err
	}
	msg, err := tcp.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringSlice(msg)
}

func (tcp *client) HashDel(key string, field string) error {
	err := tcp.w.Write("HDEL", key, field)

	if err != nil {
		return err
	}

	msg, err := tcp.r.Read()
	if err != nil {
		return err
	}

	if err = checkErr(msg); err != nil {
		return err
	}

	return nil
}

func (tcp *client) HashSet(key string, args ...string) error {
	ss := make([]string, len(args)+2)
	ss[0] = "HSET"
	ss[1] = key

	for i, v := range args {
		ss[i+2] = v
	}
	err := tcp.w.WriteStringSlice(ss)
	if err != nil {
		return err
	}
	msg, err := tcp.r.Read()

	if err != nil {
		return err
	}

	if err = checkErr(msg); err != nil {
		return err
	}

	return nil
}
