package iqdb

import (
	"bufio"
	"errors"
	"github.com/ravlio/iqdb/redis"
	"github.com/ravlio/iqdb/tcp"
	proto "github.com/ravlio/iqdb/protocol"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sync"
	"time"
	//"fmt"
)

var ErrKeyNotFound = errors.New("key not found")
var ErrKeyTypeError = errors.New("wrong key type")
var ErrListIndexError = errors.New("wrong _list index")
var ErrListOutOfBounds = errors.New("_list range out of bounds")
var ErrHashKeyNotFound = errors.New("_hash key not found")
var ErrHashKeyValueMismatch = errors.New("_hash keys and values mismatch")

// Three types of storage items
const (
	dataTypeKV   = 1
	dataTypeList = 2
	dataTypeHash = 3
)

type Options struct {
	RedisPort int
	TCPPort   int
	HTTPPort  int
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
	Opts  *Options
	// Error channel for goroutines
	Errch chan error
	// Using distributed hashed map
	distmap *distmap
	// TTL tree with scheduler
	ttl *ttlTree
	// Time callback for back to the future (ttl testing purposes)
	timeCb     func() time.Time
	aof        *os.File
	aofW       *io.Writer
	aofBuf     *bufio.Writer
	syncTicker *time.Ticker
	isSyncing  bool
	syncMx     *sync.Mutex
	redis      *redis.Redis
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
		Opts:    opts,
		distmap: NewDistmap(opts.ShardCount),
		Errch:   make(chan error),
		syncMx:  &sync.Mutex{},
	}

	db.ttl = NewTTLTree(db._removeFromHash)

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
		db.aofW = db.aof
	}

	return db, nil
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
	if iq.Opts.RedisPort > 0 {
		r := redis.NewServer(iq)
		go r.Start()
	}

	if iq.Opts.TCPPort > 0 {
		t := tcp.NewServer(iq)
		go t.Start()
	}

	if iq.Opts.HTTPPort > 0 {
		go iq.serveHTTP()
	}

	return <-iq.Errch
}

func (iq IqDB) Close() error {
	if !iq.Opts.NoAsync {
		iq.flushAOFBuffer()
	}

	if iq.Opts.RedisPort > 0 {
		iq.redis.Stop()

	}

	if iq.Opts.TCPPort > 0 {
		iq.tcp.Stop()
	}
	return iq.aof.Close()
}

func (iq *IqDB) serveHTTP() {
	log.Info("Starting HTTP server ...")

	log.Infof("HTTP server now accept connections on port %d ...", iq.Opts.HTTPPort)
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
		op, err := proto.ReadOp(rdr)
		if err != nil && err != io.EOF {
			return err
		}

		if err == io.EOF {
			break
		}

		switch op[0] {
		case proto.OpSet:

			//println("set", "key", key, "val", val, "ttl", ttl)

			key, ttl, val, err := proto.ReadSet(rdr)
			if err != nil {
				return err
			}
			err = iq.set(key, val, ttl, false)
			if err != nil {
				return err
			}
		case proto.OpRemove:
			key, err := proto.ReadRemove(rdr)
			if err != nil {
				return err
			}

			//println("_remove", "key", key)
			err = iq._remove(key, false)
			if err != nil {
				return err
			}
		case proto.OpTTL:
			key, ttl, err := proto.ReadTTL(rdr)
			if err != nil {
				return err
			}

			//println("ttl", "key", key, "ttl", ttl)

			err = iq._ttl(key, ttl, false)
			if err != nil {
				return err
			}
		case proto.OpListPush:
			key, vals, err := proto.ReadListPush(rdr)

			//println("_listPush", "key", key, "vals", fmt.Sprintf("%+v", vals))

			_, err = iq._listPush(key, vals, false)
			if err != nil {
				return err
			}
		case proto.OpListPop:
			key, err := proto.ReadListPop(rdr)
			if err != nil {
				return err
			}

			//println("_listPop", "key", key)

			_, err = iq._listPop(key, false)
			if err != nil {
				return err
			}
		case proto.OpHashDel:
			key, field, err := proto.ReadHashDel(rdr)
			if err != nil {
				return err
			}

			//println("_hashDel", "key", key, "field", field)
			err = iq._hashDel(key, field, false)
			if err != nil {
				return err
			}
		case proto.OpHashSet:
			key, vals, err := proto.ReadHashSet(rdr)

			//println("_hashSet", "key", key, "fields", fmt.Sprintf("%+v", vals))

			err = iq._hashSet(key, vals, false)
			if err != nil {
				return err
			}
		}

	}

	return nil
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
