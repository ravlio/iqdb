package redis

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

type Reader struct {
	r *bufio.Reader
}

func NewReader(r *bufio.Reader) *Reader {
	return &Reader{
		r: r,
	}
}

func (r *Reader) Read() (*Message, error) {
	return read(r.r)
}

// Redis protocol reader
func read(r *bufio.Reader) (*Message, error) {
	line, e := r.ReadBytes('\n')

	if e != nil {
		return nil, e
	}

	line = line[:len(line)-2]
	msgtype := string(line[0])
	data := string(line[1:])

	switch msgtype {
	case TypeString:
		return &Message{
			Type:   TypeString,
			String: data,
		}, nil

	case TypeError:
		return &Message{
			Type: TypeError,
			Err:  errors.New(data),
		}, nil
	case TypeInteger:
		c, err := strconv.ParseInt(data, 10, 64)
		if err != nil {
			return nil, err
		}
		return &Message{
			Type: TypeInteger,
			Int:  c,
		}, nil
	case TypeArray:
		l, e := strconv.Atoi(data)
		if e != nil {
			return nil, e
		}

		if l == -1 {
			return &Message{
				Type: TypeArray,
				Arr:  nil,
			}, nil
		}

		ret := make([]*Message, l)
		for i := 0; i < l; i++ {
			m, err := read(r)
			if err != nil {
				return nil, err
			}
			ret[i] = m
		}
		return &Message{
			Type: TypeArray,
			Arr:  ret,
		}, nil

	case TypeBulk:
		l, e := strconv.Atoi(data)
		if e != nil {
			return nil, e
		}

		if l < 0 {
			return &Message{
				Bulk: nil,
				Type: TypeBulk,
			}, nil
		}

		buf := make([]byte, l+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return &Message{
			Bulk: buf[:l],
			Type: TypeBulk,
		}, nil
	}

	return nil, errors.New("error parsing Redis protocol")
}
