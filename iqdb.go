package iqdb

import (
	"net"
	"strconv"
	"github.com/ravlio/iqdb/redis"
	log "github.com/sirupsen/logrus"
	"bufio"
	"errors"
	"sync"
	"time"
	"os"
	"io"
	"encoding/binary"
	"fmt"
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
	aofBuf     *bufio.Writer
	syncTicker *time.Ticker
	isSyncing  bool
	syncMx     *sync.Mutex
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
	// go 1.9+ sync.Map
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
		db.syncTicker = time.NewTicker(opts.SyncPeriod)
		go db.runSyncer()

	}

	return db, nil
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
			log.Error("tcp error", err)
		}
		go iq.handleConnection(conn)
	}
}

func (iq *IqDB) handleConnection(c net.Conn) {
	reader := redis.NewReader(bufio.NewReader(c))
	writer := redis.NewWriter(c)

	for {
		// TODO make debug info
		_, err := reader.Read()
		if err != nil {
			log.Error(err)
			return
		}
		err = writer.Write(errors.New("sdfdsf"))
		if err != nil {
			log.Error(err)
			return
		}
	}
}

// Write operation with key to file/buffer
func (iq *IqDB) writeKeyOp(op byte, key string, arg ...string) error {
	var w io.Writer

	if !iq.opts.NoAsync {
		w = iq.aofBuf
	} else {
		w = iq.aof
	}

	iq.syncMx.Lock()
	defer iq.syncMx.Unlock()

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

	switch op {
	case opSet:
		var err error
		var ttl int

		if arg == nil || len(arg) < 2 {
			ttl = 0
		} else {
			ttl, err = strconv.Atoi(arg[1])
			if err != nil {
				return err
			}
		}

		ttlb := make([]byte, 8)

		binary.LittleEndian.PutUint64(ttlb, uint64(ttl))
		_, err = w.Write(ttlb)
		if err != nil {
			return err
		}

		// value
		kb := []byte(arg[0])
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

	case opTTL:
		var err error
		var ttl int

		if arg == nil {
			ttl = 0
		} else {
			ttl, err = strconv.Atoi(arg[0])
			if err != nil {
				return nil
			}
		}

		ttlb := make([]byte, 8)

		binary.LittleEndian.PutUint64(ttlb, uint64(ttl))
		_, err = w.Write(ttlb)
		if err != nil {
			return err
		}
	case opListPush:
	case opHashSet:

		an := make([]byte, 8)
		binary.LittleEndian.PutUint64(an, uint64(len(arg)))
		_, err = w.Write(an)
		if err != nil {
			return err
		}
		for _, v := range arg {
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
	case opHashDel:
		// field
		kb := []byte(arg[0])
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

		n, err := rdr.Read(op)

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
			println("set", "key", key, "val", val, "ttl", ttl)

			err = iq.set(key, val, time.Duration(ttl)*time.Second, false)
			if err != nil {
				return err
			}
		case opRemove:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			println("remove", "key", key)
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

			println("ttl", "key", key, "ttl", ttl)

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

			println("listPush", "key", key, "vals", fmt.Sprintf("%+v", vals))

			_, err = iq.listPush(key, vals, false)
			if err != nil {
				return err
			}
		case opListPop:
			key, err := readString(rdr)
			if err != nil {
				return err
			}

			println("listPop", "key", key)

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

			println("hashDel", "key", key, "field", field)
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
			for i := 0; i < int(n); i++ {
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

			println("hashSet", "key", key, "fields", fmt.Sprintf("%+v", vals))

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

	_, err := rdr.Read(l)
	if err != nil {
		return nil, err
	}

	//println(binary.LittleEndian.Uint64(l))
	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = rdr.Read(b)
	//println(string(b))
	if err != nil {
		return nil, err
	}

	return b, nil
}

func readUint64(rdr io.Reader) (uint64, error) {
	i := make([]byte, 8)
	_, err := rdr.Read(i)
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
