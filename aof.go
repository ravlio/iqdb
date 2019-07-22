package iqdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"time"
)

func (iq *IqDB) writeKeyOp(op byte, key string) error {
	// op
	_, err := iq.aofW.Write([]byte{op})
	if err != nil {
		return err
	}
	// key
	kb := []byte(key)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = iq.aofW.Write(l)
	if err != nil {
		return err
	}
	_, err = iq.aofW.Write(kb)
	if err != nil {
		return err
	}

	return nil
}

func (iq *IqDB) writeRemove(key string) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	return iq.writeKeyOp(opRemove, key)
}

func (iq *IqDB) writeListPop(key string) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	return iq.writeKeyOp(opListPop, key)
}

func (iq *IqDB) writeSet(key, value string, ttl time.Duration) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	err := iq.writeKeyOp(opSet, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = iq.aofW.Write(ttlb)
	if err != nil {
		return err
	}
	// value
	kb := []byte(value)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))

	_, err = iq.aofW.Write(l)
	if err != nil {
		return err
	}
	_, err = iq.aofW.Write(kb)
	if err != nil {
		return err
	}
	return nil
}

func (iq *IqDB) writeTTL(key string, ttl time.Duration) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	err := iq.writeKeyOp(opTTL, key)
	if err != nil {
		return err
	}

	ttlb := make([]byte, 8)

	binary.LittleEndian.PutUint64(ttlb, uint64(ttl.Seconds()))
	_, err = iq.aofW.Write(ttlb)
	if err != nil {
		return err
	}

	return nil
}

func (iq *IqDB) writeListPush(key string, args ...string) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	err := iq.writeKeyOp(opListPush, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = iq.aofW.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// key
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = iq.aofW.Write(l)
		if err != nil {
			return err
		}
		_, err = iq.aofW.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func (iq *IqDB) writeHashSet(key string, args ...string) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	err := iq.writeKeyOp(opHashSet, key)
	if err != nil {
		return err
	}

	an := make([]byte, 8)
	binary.LittleEndian.PutUint64(an, uint64(len(args)))
	_, err = iq.aofW.Write(an)
	if err != nil {
		return err
	}
	for _, v := range args {
		// field or value
		kb := []byte(v)
		l := make([]byte, 8)
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		_, err = iq.aofW.Write(l)
		if err != nil {
			return err
		}
		_, err = iq.aofW.Write(kb)
		if err != nil {
			return err
		}
	}

	return nil
}

func (iq *IqDB) writeHashDel(key, f string) error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	err := iq.writeKeyOp(opHashDel, key)
	if err != nil {
		return err
	}

	// field
	kb := []byte(f)
	l := make([]byte, 8)
	binary.LittleEndian.PutUint64(l, uint64(len(kb)))
	_, err = iq.aofW.Write(l)
	if err != nil {
		return err
	}
	_, err = iq.aofW.Write(kb)
	if err != nil {
		return err
	}

	return nil
}

func (iq *IqDB) readAOF() error {
	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

	f, err := os.Open(iq.fname)

	fi, _ := f.Stat()
	if fi.Size() == 0 {
		return nil
	}

	if err != nil {
		return err
	}

	rdr := bufio.NewReader(f)

	for {
		op := make([]byte, 1)

		n, err := io.ReadFull(rdr, op)

		if err != nil && err != io.EOF {
			return err
		}

		if n == 0 || err == io.EOF {
			break
		}

		switch op[0] {
		case opSet:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			ttl, err := readUint64(rdr)
			if err != nil {
				return err
			}

			val, err := readString(rdr)
			if err != nil {
				return err
			}

			err = iq.set(key, val, time.Duration(ttl)*time.Second, false)
			if err != nil {
				return err
			}
		case opRemove:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			err = iq.remove(key, false)
			if err != nil {
				return err
			}
		case opTTL:
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			ttl, err := readUint64(rdr)
			if err != nil {
				return err
			}

			err = iq._ttl(key, time.Duration(ttl)*time.Second, false)
			if err != nil {
				return err
			}
		case opListPush:
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			n, err := readUint64(rdr)
			if err != nil {
				return err
			}

			vals := make([]string, int(n))
			for i := 0; i < int(n); i++ {
				v, err := readString(rdr)
				if err != nil {
					return err
				}

				vals[i] = v
			}

			_, err = iq.listPush(key, vals, false)
			if err != nil {
				return err
			}
		case opListPop:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			_, err = iq.listPop(key, false)
			if err != nil {
				return err
			}
		case opHashDel:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			field, err := readString(rdr)
			if err != nil {
				return err
			}

			err = iq.hashDel(key, field, false)
			if err != nil {
				return err
			}
		case opHashSet:
			// TODO check eoh
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			n, err := readUint64(rdr)
			if err != nil {
				return err
			}

			vals := make(map[string]string, int(n))
			for i := 0; i < int(n/2); i++ {
				f, err := readString(rdr)
				if err != nil {
					return err
				}
				v, err := readString(rdr)
				if err != nil {
					return err
				}

				vals[f] = v
			}

			err = iq.hashSet(key, vals, false)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func readString(rdr io.Reader) (string, error) {
	b, err := readBytes(rdr)

	if err != nil {
		return "", err
	}

	return string(b), nil
}

func readBytes(rdr io.Reader) ([]byte, error) {
	var l = make([]byte, 8)

	_, err := io.ReadFull(rdr, l)
	if err != nil {
		return nil, err
	}

	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = io.ReadFull(rdr, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func readUint64(rdr io.Reader) (uint64, error) {
	i := make([]byte, 8)
	_, err := io.ReadFull(rdr, i)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(i), nil
}

func (iq *IqDB) runSyncer() {
	for range iq.syncTicker.C {
		iq.flushAOFBuffer()
	}
}

func (iq *IqDB) flushAOFBuffer() {
	iq.syncMx.Lock()
	iq.aofBuf.Flush()
	iq.syncMx.Unlock()
}
