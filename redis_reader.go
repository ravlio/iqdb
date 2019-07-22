package iqdb

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

type redisReader struct {
	r *bufio.Reader
}

func newRedisReader(r *bufio.Reader) *redisReader {
	return &redisReader{
		r: r,
	}
}

func (r *redisReader) Read() (*redisMessage, error) {
	return read(r.r)
}

// RedisClient protocol redisReader
func read(r *bufio.Reader) (*redisMessage, error) {
	line, e := r.ReadBytes('\n')

	if e != nil {
		return nil, e
	}

	line = line[:len(line)-2]
	msgtype := redisType(line[0])
	data := string(line[1:])

	switch msgtype {
	case redisTypeString:
		return &redisMessage{
			Type:   redisTypeString,
			String: data,
		}, nil

	case redisTypeError:
		return &redisMessage{
			Type: redisTypeError,
			Err:  errors.New(data),
		}, nil
	case redisTypeInteger:
		c, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return nil, err
		}
		return &redisMessage{
			Type: redisTypeInteger,
			Int:  c,
		}, nil
	case redisTypeArray:
		l, e := strconv.Atoi(data)
		if e != nil {
			return nil, e
		}

		if l == -1 {
			return &redisMessage{
				Type: redisTypeArray,
				Arr:  nil,
			}, nil
		}

		ret := make([]*redisMessage, l)
		for i := 0; i < l; i++ {
			m, err := read(r)
			if err != nil {
				return nil, err
			}
			ret[i] = m
		}
		return &redisMessage{
			Type: redisTypeArray,
			Arr:  ret,
		}, nil

	case redisTypeBulk:
		l, e := strconv.Atoi(data)
		if e != nil {
			return nil, e
		}

		if l < 0 {
			return &redisMessage{
				Bulk: nil,
				Type: redisTypeBulk,
			}, nil
		}

		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return &redisMessage{
			Bulk: buf[:l],
			Type: redisTypeBulk,
		}, nil
	}

	return nil, errors.New("error parsing RedisClient protocol")
}
