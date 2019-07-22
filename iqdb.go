package iqdb

import (
	"bufio"
	"errors"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
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
	RedisPort int
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

var timeFunc = func() time.Time {
	return time.Now()
}

type IqDB struct {
	fname string
	// TCP reader and writer
	redis *redisServer
	opts  *Options
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
	}

	db.ttl = newTTLTree(db.removeFromHash)

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

// We can redefine time to force TTL expiring
func SetTimeFunc(cb func() time.Time) {
	timeFunc = cb
}

func (iq *IqDB) Start() error {
	if iq.opts.RedisPort > 0 {
		iq.redis = newRedisServer(iq.opts.RedisPort, iq)
		go iq.redis.Serve()
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

	if iq.opts.RedisPort > 0 {
		iq.redis.Stop()
	}
	return iq.aof.Close()
}

func (iq *IqDB) serveHTTP() {
	log.Info("Starting HTTP server ...")

	log.Infof("HTTP server now accept connections on port %d ...", iq.opts.HTTPPort)
}
