package iqdb

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"time"
)

type redisType string

var ErrRedisWrongArgNum = errors.New("wrong arguments number")
var ErrRedisWrongTTL = errors.New("wrong TTL")
var ErrRedisUnknownParseError = errors.New("unknown parse error")

const (
	redisTypeString  redisType = "+"
	redisTypeError   redisType = "-"
	redisTypeInteger redisType = ":"
	redisTypeArray   redisType = "*"
	redisTypeBulk    redisType = "$"
)

// RedisClient protocol format
type redisMessage struct {
	Type   redisType
	String string
	Int    int64
	Arr    []*redisMessage
	Bulk   []byte
	Err    error
}

type RedisClient struct {
	r *redisReader
	w *redisWriter
}

func NewRedisClient(addr string) (*RedisClient, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	tcp := &RedisClient{
		r: newRedisReader(bufio.NewReader(c)),
		w: newRedisWriter(c),
	}

	return tcp, nil
}

func (cl *RedisClient) Get(key string) (string, error) {
	cl.w.write("GET", key)
	msg, err := cl.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (cl *RedisClient) Set(key, value string, ttl ...time.Duration) error {
	var err error
	if ttl != nil {
		err = cl.w.write("SET", key, value, ttl[0])
	} else {
		err = cl.w.write("SET", key, value)
	}

	if err != nil {
		return err
	}
	_, err = cl.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (cl *RedisClient) Remove(key string) error {
	err := cl.w.write("DEL", key)

	if err != nil {
		return err
	}

	_, err = cl.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (cl *RedisClient) TTL(key string, ttl time.Duration) error {
	err := cl.w.write("TTL", key, ttl)

	if err != nil {
		return err
	}

	_, err = cl.r.Read()
	if err != nil {
		return err
	}

	return nil
}

func (cl *RedisClient) Keys() chan<- string {
	panic("implement me")
}

func (cl *RedisClient) ListLen(key string) (int, error) {
	err := cl.w.write("LLEN", key)

	if err != nil {
		return 0, err
	}

	m, err := cl.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(m); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(m)
}

func (cl *RedisClient) ListIndex(key string, index int) (string, error) {
	err := cl.w.write("LINDEX", key, index)

	if err != nil {
		return "", err
	}
	msg, err := cl.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (cl *RedisClient) ListPush(key string, value ...string) (int, error) {
	ss := make([]string, len(value)+2)
	ss[0] = "LPUSH"
	ss[1] = key

	for i, v := range value {
		ss[i+2] = v
	}
	err := cl.w.writeStringSlice(ss)
	if err != nil {
		return 0, err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(msg); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(msg)
}

func (cl *RedisClient) ListPop(key string) (int, error) {
	err := cl.w.write("LPOP", key)

	if err != nil {
		return 0, err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return 0, err
	}

	if err = checkErr(msg); err != nil {
		return 0, err
	}

	return getFirstBulkAsInt(msg)
}

func (cl *RedisClient) ListRange(key string, from, to int) ([]string, error) {
	err := cl.w.write("LRANGE", key, from, to)

	if err != nil {
		return nil, err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringSlice(msg)
}

func (cl *RedisClient) HashGet(key string, field string) (string, error) {
	err := cl.w.write("HGET", key, field)
	if err != nil {
		return "", err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return "", err
	}

	if err = checkErr(msg); err != nil {
		return "", err
	}

	return getFirstBulkAsString(msg)
}

func (cl *RedisClient) HashGetAll(key string) (map[string]string, error) {
	err := cl.w.write("HGETALL", key)
	if err != nil {
		return nil, err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringMap(msg)
}

func (cl *RedisClient) HashKeys(key string) ([]string, error) {
	err := cl.w.write("HKEYS", key)
	if err != nil {
		return nil, err
	}
	msg, err := cl.r.Read()
	if err != nil {
		return nil, err
	}

	if err = checkErr(msg); err != nil {
		return nil, err
	}

	return getFirstBulkAsStringSlice(msg)
}

func (cl *RedisClient) HashDel(key string, field string) error {
	err := cl.w.write("HDEL", key, field)

	if err != nil {
		return err
	}

	msg, err := cl.r.Read()
	if err != nil {
		return err
	}

	if err = checkErr(msg); err != nil {
		return err
	}

	return nil
}

func (cl *RedisClient) HashSet(key string, args ...string) error {
	ss := make([]string, len(args)+2)
	ss[0] = "HSET"
	ss[1] = key

	for i, v := range args {
		ss[i+2] = v
	}
	err := cl.w.writeStringSlice(ss)
	if err != nil {
		return err
	}
	msg, err := cl.r.Read()

	if err != nil {
		return err
	}

	if err = checkErr(msg); err != nil {
		return err
	}

	return nil
}

func getFirstBulkAsString(msg *redisMessage) (string, error) {
	if msg.Type != redisTypeArray {
		return "", ErrRedisUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return "", ErrRedisUnknownParseError
	}

	return string(msg.Arr[0].Bulk), nil
}

func getFirstBulkAsStringSlice(msg *redisMessage) ([]string, error) {
	if msg.Type != redisTypeArray {
		return nil, ErrRedisUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return nil, ErrRedisUnknownParseError
	}

	r := make([]string, len(msg.Arr))
	for i := range msg.Arr {
		r[i] = string(msg.Arr[i].Bulk)
	}

	return r, nil
}

func getFirstBulkAsStringMap(msg *redisMessage) (map[string]string, error) {
	if msg.Type != redisTypeArray {
		return nil, ErrRedisUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return nil, ErrRedisUnknownParseError
	}

	r := make(map[string]string)
	for i := 0; i < len(msg.Arr); i += 2 {
		r[string(msg.Arr[i].Bulk)] = string(msg.Arr[i+1].Bulk)
	}

	return r, nil
}

func getFirstBulkAsInt(msg *redisMessage) (int, error) {
	if msg.Type != redisTypeArray {
		return 0, ErrRedisUnknownParseError
	}

	if len(msg.Arr) == 0 {
		return 0, ErrRedisUnknownParseError
	}

	return strconv.Atoi(string(msg.Arr[0].Bulk))
}

func checkErr(msg *redisMessage) error {
	if msg.Arr[0].Type == redisTypeError {
		return msg.Arr[0].Err
	}

	return nil
}
