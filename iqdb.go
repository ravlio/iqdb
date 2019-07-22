package iqdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/ravlio/iqdb/redis"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrKeyTypeError = errors.New("wrong key type")
var ErrListIndexError = errors.New("wrong list index")
var ErrListOutOfBounds = errors.New("list range out of bounds")
var ErrHashKeyNotFound = errors.New("hash key not found")
var ErrHashKeyValueMismatch = errors.New("hash keys and values mismatch")

// Three types of storage items
const (
	dataTypeKV   = 1
	dataTypeList = 2
	dataTypeHash = 3
)

const (
	opSet      = 1
	opRemove   = 2
	opTTL      = 3
	opListPush = 4
	opListPop  = 5
	opHashDel  = 6
	opHashSet  = 7
)

type Client interface {
	Get(key string) (string, error)
	Set(key, value string, ttl ...time.Duration) error
	Remove(key string) error
	TTL(key string, ttl time.Duration) error
	Keys() chan<- string
	ListLen(key string) (int, error)
	ListIndex(key string, index int) (string, error)
	ListPush(key string, value ...string) (int, error)
	ListPop(key string) (int, error)
	ListRange(key string, from, to int) ([]string, error)
	HashGet(key string, field string) (string, error)
	HashGetAll(key string) (map[string]string, error)
	HashKeys(key string) ([]string, error)
	HashDel(key string, field string) error
	HashSet(key string, args ...string) error
}

type Options struct {
	TCPPort  int
	HTTPPort int
	// Default TTL. Used if >0
	TTL        time.Duration
	ShardCount int
	// Predefined cluster size, right now the only way
	ClusterSize int
	// Disable async
	NoAsync bool
	// Buffer sync period
	SyncPeriod time.Duration
}

type IqDB struct {
	fname string
	// TCP listener
	ln net.Listener
	// TCP reader and writer
	reader *redis.Reader
	writer *redis.Writer
	opts   *Options
	// Error channel for goroutines
	errch chan error
	// Using distributed hashed map
	distmap *distmap
	// TTL tree with scheduler
	ttl *ttlTree
	// Time callback for back to the future (ttl testing purposes)
	timeCb     func() time.Time
	aof        *os.File
	aofW       io.Writer
	aofBuf     *bufio.Writer
	syncTicker *time.Ticker
	isSyncing  bool
	syncMx     *sync.Mutex
	stopc      chan struct{}
}

// KeyValue entity
// Contains all types as pointers so they would not occupy much memory, just pointers
type KV struct {
	ttl      time.Duration
	dataType int
	Value    string
	list     *list
	hash     *hash
}

type list struct {
	// Mutex is needed upon writing
	mx   *sync.RWMutex
	list []string
}

type hash struct {
	hash *sync.Map
}

func Open(fname string, opts *Options) (*IqDB, error) {
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}

	if opts.ClusterSize <= 0 {
		opts.ClusterSize = 1
	}

	if opts.SyncPeriod == 0 {
		opts.SyncPeriod = time.Second
	}

	db := &IqDB{
		fname:   fname,
		opts:    opts,
		distmap: NewDistmap(opts.ShardCount),
		errch:   make(chan error),
		syncMx:  &sync.Mutex{},
		stopc:   make(chan struct{}, 1),
	}

	db.ttl = NewTTLTree(db.removeFromHash)

	aof, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	db.aof = aof
	db.aofBuf = bufio.NewWriter(aof)

	err = db.readAOF()

	if err != nil {
		return nil, err
	}

	if !opts.NoAsync && opts.SyncPeriod > 0 {
		db.aofW = db.aofBuf
		db.syncTicker = time.NewTicker(opts.SyncPeriod)
		go db.runSyncer()

	} else {
		db.aofW = aof
	}

	return db, nil
}

func MakeTCPClient(addr string) (Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	tcp := &tcp{
		r: redis.NewReader(bufio.NewReader(c)),
		w: redis.NewWriter(c),
	}

	return tcp, nil
}

func MakeHTTPClient() Client {
	return &http{}
}

var timeFunc = func() time.Time {
	return time.Now()
}

// We can redefine time to force TTL expiring
func SetTimeFunc(cb func() time.Time) {
	timeFunc = cb
}

func (iq *IqDB) Start() error {
	if iq.opts.TCPPort > 0 {
		go iq.serveTCP()
	}

	if iq.opts.HTTPPort > 0 {
		go iq.serveHTTP()
	}

	return <-iq.errch
}

func (iq IqDB) Close() error {
	if !iq.opts.NoAsync {
		iq.flushAOFBuffer()
	}

	if iq.opts.TCPPort > 0 {
		iq.StopTCP()
		//iq.ln.Close()
		// not so fast
		/*err := iq.ln.Close()
		if err != nil {
			return err
		}*/
	}
	return iq.aof.Close()
}

func (iq *IqDB) serveHTTP() {
	log.Info("Starting HTTP server ...")

	log.Infof("HTTP server now accept connections on port %d ...", iq.opts.HTTPPort)
}

func (iq *IqDB) serveTCP() {
	var err error

	log.Info("Starting TCP server ...")

	iq.ln, err = net.Listen("tcp", ":"+strconv.Itoa(iq.opts.TCPPort))
	if err != nil {
		iq.errch <- err
		return
	}

	log.Infof("TCP server now accept connections on port %d ...", iq.opts.TCPPort)

	for {
		conn, err := iq.ln.Accept()
		if err != nil {
			select {
			case <-iq.stopc:
				iq.ln.Close()
				return
			default:
				log.Error("tcp error", err)

			}
		}
		go iq.handleConnection(conn)
	}
}

func (iq *IqDB) StopTCP() {
	iq.stopc <- struct{}{}
}

func (iq *IqDB) handleConnection(c net.Conn) {
	reader := redis.NewReader(bufio.NewReader(c))
	writer := redis.NewWriter(c)

	for {
		msg, err := reader.Read()

		switch msg.Type {
		case redis.TypeArray:
			switch msg.Arr[0].Type {
			case redis.TypeBulk:
				switch string(msg.Arr[0].Bulk) {
				case "SET":
					if len(msg.Arr) < 3 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					val := string(msg.Arr[2].Bulk)

					var ttl time.Duration
					if len(msg.Arr) == 4 {
						ttli, err := strconv.Atoi(string(msg.Arr[3].Bulk))
						ttl = time.Duration(ttli)
						if err != nil {
							err = writer.Write(redis.ErrWrongTTL)
							continue
						}
					}

					err = iq.Set(key, val, ttl)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "GET":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := iq.Get(key)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue
				case "DEL":
					if len(msg.Arr) < 1 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					err := iq.Remove(key)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "TTL":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					var ttl time.Duration
					ttli, err := strconv.Atoi(string(msg.Arr[2].Bulk))
					ttl = time.Duration(ttli)
					if err != nil {
						err = writer.Write(redis.ErrWrongTTL)
						continue
					}

					err = iq.TTL(key, ttl)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HGET":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					v, err := iq.HashGet(key, field)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "HSET":
					if len(msg.Arr) < 3 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					err := iq.HashSet(key, fields...)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HGETALL":
					if len(msg.Arr) < 1 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := iq.HashGetAll(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					r := make([]string, len(v)*2)

					i := 0
					for k, a := range v {
						r[i] = k
						r[i+1] = a
						i += 2
					}

					writer.WriteStringSlice(r)
					continue

				case "HDEL":
					if len(msg.Arr) < 3 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					err := iq.HashDel(key, field)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HKEYS":
					if len(msg.Arr) < 1 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := iq.HashKeys(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.WriteStringSlice(v)
					continue

				case "LLEN":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := iq.ListLen(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LINDEX":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					index, err := strconv.Atoi(string(msg.Arr[2].Bulk))

					if err != nil {
						writer.Write(err)
						continue
					}

					v, err := iq.ListIndex(key, index)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LPOP":
					if len(msg.Arr) < 2 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					if err != nil {
						writer.Write(err)
						continue
					}

					v, err := iq.ListPop(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LRANGE":
					if len(msg.Arr) < 4 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					from, err := strconv.Atoi(string(msg.Arr[2].Bulk))

					if err != nil {
						writer.Write(err)
						continue
					}

					to, err := strconv.Atoi(string(msg.Arr[3].Bulk))

					if err != nil {
						writer.Write(err)
						continue
					}

					v, err := iq.ListRange(key, from, to)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.WriteStringSlice(v)
					continue

				case "LPUSH":
					if len(msg.Arr) < 3 {
						err = writer.Write(redis.ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					i, err := iq.ListPush(key, fields...)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(i)
					continue
				}
			}
		}

		err = writer.Write(redis.ErrUnknownParseError)
		if err != nil {
			log.Error(err)
			return
		}
	}
}

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
	//_, err := rdr.Read(l)
	if err != nil {
		return nil, err
	}

	//print("len ", binary.LittleEndian.Uint64(l), " ")
	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = io.ReadFull(rdr, b)
	//println("v", string(b))
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
