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
	opHashDel  = 5
	opHashSet  = 6
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

func MakeServer(fname string, opts *Options) (*IqDB, error) {
	if opts.ShardCount <= 0 {
		opts.ShardCount = 1
	}

	if opts.ClusterSize <= 0 {
		opts.ClusterSize = 1
	}

	if opts.SyncPeriod == 0 {
		opts.SyncPeriod = time.Second
	}

	aof, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	db := &IqDB{
		opts:    opts,
		distmap: NewDistmap(opts.ShardCount),
		errch:   make(chan error),
		aof:     aof,
		aofBuf:  bufio.NewWriter(aof),
	}

	if !opts.NoAsync && opts.SyncPeriod > 0 {
		db.syncTicker = time.NewTicker(opts.SyncPeriod)
		db.syncMx = &sync.Mutex{}
		go db.runSyncer()

	}
	db.ttl = NewTTLTree(db.removeFromHash)

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
func (iqdb *IqDB) writeKeyOp(op byte, key string, arg ...string) error {
	var w io.Writer

	if !iqdb.opts.NoAsync {
		w = iqdb.aofBuf
	} else {
		w = iqdb.aof
	}

	iqdb.syncMx.Lock()
	defer iqdb.syncMx.Unlock()

	// op
	w.Write([]byte(op))
	// key
	kb := []byte(key)
	var l []byte
	binary.LittleEndian.PutUint16(l, uint16(len(kb)))
	w.Write(l)
	w.Write(kb)

	// args

	for _, v := range arg {
		// key
		kb := []byte(v)
		var l []byte
		binary.LittleEndian.PutUint64(l, uint64(len(kb)))
		w.Write(l)
		w.Write(kb)
	}
}

func (iqdb *IqDB) readOps() error {
	iqdb.syncMx.Lock()
	defer iqdb.syncMx.Unlock()

	rdr := bufio.NewReader(iqdb.aof)

	for {
		var op []byte

		n, err := iqdb.aof.Read(op)
		if err != nil && err != io.EOF {
			return err
		}

		if n == 0 {
			break
		}

		switch op {
		case opSet:
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			val, err := readString(rdr)
			if err != nil {
				return err
			}
		case opRemove:
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			val, err := readString(rdr)
			if err != nil {
				return err
			}
		case opTTL:
			key, err := readString(rdr)
			if err != nil {
				return err
			}
			ttl, err := readString(rdr)
			if err != nil {
				return err
			}
		}
		}
	}
}

func readString(rdr io.Reader) (string, error) {
	var l = make([]byte, 4)

	_, err := rdr.Read(l)
	if err != nil {
		return "", err
	}

	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = rdr.Read(b)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func readBytes(rdr io.Reader) ([]byte, error) {
	var l = make([]byte, 4)

	_, err := rdr.Read(l)
	if err != nil {
		return "", err
	}

	b := make([]byte, binary.LittleEndian.Uint64(l))

	_, err = rdr.Read(b)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (iqdb *IqDB) runSyncer() {
	for range iqdb.syncTicker.C {
		iqdb.flushAOFBuffer()
	}
}

func (iqdb *IqDB) flushAOFBuffer() {
	iqdb.syncMx.Lock()
	iqdb.aofBuf.Flush()
	iqdb.syncMx.Unlock()
}
