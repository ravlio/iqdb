package protocol

import (
	"bufio"
	"time"
	"io"
)

func NewReader() error {
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
			//println("set", "key", key, "val", val, "ttl", ttl)

			err = iq.set(key, val, time.Duration(ttl)*time.Second, false)
			if err != nil {
				return err
			}
		case opRemove:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			//println("remove", "key", key)
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

			//println("ttl", "key", key, "ttl", ttl)

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

			//println("listPush", "key", key, "vals", fmt.Sprintf("%+v", vals))

			_, err = iq.listPush(key, vals, false)
			if err != nil {
				return err
			}
		case opListPop:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			//println("listPop", "key", key)

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

			//println("hashDel", "key", key, "field", field)
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

			//println("hashSet", "key", key, "fields", fmt.Sprintf("%+v", vals))

			err = iq.hashSet(key, vals, false)
			if err != nil {
				return err
			}
		}

	}
	return nil

}