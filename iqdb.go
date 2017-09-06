package iqdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/ravlio/iqdb/redis"
	"github.com/ravlio/iqdb/tcp"
	log "github.com/sirupsen/logrus"
	"io"
	"os"
	"sync"
	"time"
	//"fmt"
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


type Options struct {
	RedisPort int
	TCPPort int
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
	Opts   *Options
	// Error channel for goroutines
	Errch chan error
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
		err := iq.redis.Stop()
		if err != nil {
			return err
		}
	}

	if iq.Opts.TCPPort > 0 {
		err := iq.tcp.Stop()
		if err != nil {
			return err
		}
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
