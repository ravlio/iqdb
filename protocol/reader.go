package protocol

import (
	"time"
	"io"
	"errors"
)

var ErrNotOk = errors.New("not ok")

type proto struct {
	io io.ReadWriteCloser
}

func ReadOK(r io.Reader) (error) {
	op := make([]byte, 1)

	_, err := io.ReadFull(r, op)

	if err != nil {
		return err
	}

	if op[0] != OpOk {
		return ErrNotOk
	}
	return nil
}

func ReadOp(r io.Reader) ([]byte, error) {
	op := make([]byte, 1)

	_, err := io.ReadFull(r, op)

	return op, err
}

func ReadGet(r io.Reader) (string, error) {
	key, err := readString(r)
	if err != nil {
		return "",  err
	}

	return key, nil
}

func ReadSet(r io.Reader) (string, time.Duration, string, error) {
	key, err := readString(r)
	if err != nil {
		return "", 0, "", err
	}

	ttl, err := readUint64(r)
	if err != nil {
		return "", 0, "", err
	}

	val, err := readString(r)
	if err != nil {
		return "", 0, "", err
	}

	return key, time.Duration(ttl) * time.Second, val, nil
}

func ReadRemove(r io.Reader) (string, error) {
	return readString(r)
}

func ReadTTL(r io.Reader) (string, time.Duration, error) {
	key, err := readString(r)
	if err != nil {
		return "", 0, err
	}
	ttl, err := readUint64(r)
	if err != nil {
		return "", 0, err
	}

	return key, time.Duration(ttl) * time.Second, nil
}

func ReadListPush(r io.Reader) (string, []string, error) {
	key, err := readString(r)
	if err != nil {
		return "", nil, err
	}
	n, err := readUint64(r)
	if err != nil {
		return "", nil, err
	}

	vals := make([]string, int(n))
	for i := 0; i < int(n); i++ {
		v, err := readString(r)
		if err != nil {
			return "", nil, err
		}

		vals[i] = v
	}

	return key, vals, nil
}

func ReadListPop(r io.Reader) (string, error) {
	return readString(r)
}

func ReadHashDel(r io.Reader) (string, string, error) {
	key, err := readString(r)
	if err != nil {
		return "", "", err
	}

	field, err := readString(r)
	if err != nil {
		return "", "", err
	}

	return key, field, nil
}

func ReadHashSet(r io.Reader) (string, map[string]string, error) {
	// TODO check eoh
	key, err := readString(r)
	if err != nil {
		return "", nil, err
	}
	n, err := readUint64(r)
	if err != nil {
		return "", nil, err
	}

	vals := make(map[string]string, int(n))
	for i := 0; i < int(n/2); i++ {
		f, err := readString(r)
		if err != nil {
			return "", nil, err
		}
		v, err := readString(r)
		if err != nil {
			return "", nil, err
		}

		vals[f] = v
	}

	return key, vals, nil
}
