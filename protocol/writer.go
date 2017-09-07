package protocol

import (
	"time"
	"io"
	"encoding/binary"
)

func WriteOK(w io.Writer) error {
	// write just op :)
	_, err := w.Write([]byte{OpError})
	if err != nil {
		return err
	}

	return nil
}

func WriteError(w io.Writer, e error) error {
	// op
	_, err := w.Write([]byte{OpError})
	if err != nil {
		return err
	}
	// error

	errb := []byte(e.Error())
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(errb)))
	_, err = w.Write(l)
	if err != nil {
		return err
	}

	_, err = w.Write(errb)
	if err != nil {
		return err
	}

	return nil
}
func WriteKeyOp(w io.Writer, op byte, key string) error {
	// op
	_, err := w.Write([]byte{op})
	if err != nil {
		return err
	}
	// key
	kb := []byte(key)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.Write(kb)
	if err != nil {
		return err
	}

	return nil
}

func WriteRemove(w io.Writer, key string) error {
	return WriteKeyOp(w,OpRemove, key)
}

func WriteListPop(w io.Writer, key string) error {
	return WriteKeyOp(w,OpListPop, key)
}

func WriteGet(w io.Writer, k string) error {
	// op
	_, err := w.Write([]byte{OpGet})
	if err != nil {
		return err
	}
	// error

	kb := []byte(k)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = w.Write(l)
	if err != nil {
		return err
	}

	_, err = w.Write(kb)
	if err != nil {
		return err
	}

	return nil
}

func WriteSet(w io.Writer, key, value string, ttl time.Duration) error {
	err := WriteKeyOp(w, OpSet, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = w.Write(ttlb)
	if err != nil {
		return err
	}
	// value
	kb := []byte(value)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))

	_, err = w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.Write(kb)
	if err != nil {
		return err
	}
	return nil
}

func WriteTTL(w io.Writer, key string, ttl time.Duration) error {
	err := WriteKeyOp(w, OpTTL, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = w.Write(ttlb)
	if err != nil {
		return err
	}

	return nil
}

func WriteListPush(w io.Writer, key string, args ...string) error {
	err := WriteKeyOp(w, OpListPush, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = w.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// key
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = w.Write(l)
		if err != nil {
			return err
		}
		_, err = w.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func WriteHashSet(w io.Writer, key string, args ...string) error {
	err := WriteKeyOp(w, OpHashSet, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = w.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// field or value
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = w.Write(l)
		if err != nil {
			return err
		}
		_, err = w.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func WriteHashDel(w io.Writer, key, f string) error {
	err := WriteKeyOp(w, OpHashDel, key)
	if err != nil {
		return err
	}

	// field
	kb := []byte(f)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.Write(kb)
	if err != nil {
		return err
	}

	return nil
}
