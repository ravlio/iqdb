package protocol

import (
	"time"
	"io"
	"encoding/binary"
	"sync"
)

type w struct {
	w io.Writer
	syncMx *sync.Mutex
}

func NewWriter(w io.Writer) *w {
	return &w{
		w: w,
		syncMx:&sync.Mutex{},
	}
}

func (w *w) WriteKeyOp(op byte, key string) error {
	// op
	_, err := w.w.Write([]byte{op})
	if err != nil {
		return err
	}
	// key
	kb := []byte(key)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = w.w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.w.Write(kb)
	if err != nil {
		return err
	}

	return nil
}

func (w *w) WriteRemove(key string) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	return w.WriteKeyOp(OpRemove, key)
}

func (w *w) WriteListPop(key string) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	return w.WriteKeyOp(OpListPop, key)
}

func (w *w) WriteSet(key, value string, ttl time.Duration) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	err := w.WriteKeyOp(OpSet, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = w.w.Write(ttlb)
	if err != nil {
		return err
	}
	// value
	kb := []byte(value)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))

	_, err = w.w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.w.Write(kb)
	if err != nil {
		return err
	}
	return nil
}

func (w *w) WriteTTL(key string, ttl time.Duration) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	err := w.WriteKeyOp(OpTTL, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = w.w.Write(ttlb)
	if err != nil {
		return err
	}

	return nil
}

func (w *w) WriteListPush(key string, args ...string) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	err := w.WriteKeyOp(OpListPush, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = w.w.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// key
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = w.w.Write(l)
		if err != nil {
			return err
		}
		_, err = w.w.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *w) WriteHashSet(key string, args ...string) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	err := w.WriteKeyOp(OpHashSet, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = w.w.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// field or value
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = w.w.Write(l)
		if err != nil {
			return err
		}
		_, err = w.w.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *w) WriteHashDel(key, f string) error {
	w.syncMx.Lock()
	defer w.syncMx.Unlock()

	err := w.WriteKeyOp(OpHashDel, key)
	if err != nil {
		return err
	}

	// field
	kb := []byte(f)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = w.w.Write(l)
	if err != nil {
		return err
	}
	_, err = w.w.Write(kb)
	if err != nil {
		return err
	}

	return nil
}
