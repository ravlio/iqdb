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
	TCPPort    int
	HTTPPort   int
	// Default TTL. Used if >0
	TTL        time.Duration
	ShardCount int
}

type IqDB struct {
	// TCP listener
	ln      net.Listener
	// TCP reader and writer
	reader  *redis.Reader
	writer  *redis.Writer
	opts    *Options
	// Error channel for goroutines
	errch   chan error
	// Using distributed hashed map
	distmap *distmap
	// TTL tree with scheduler
	ttl     *ttlTree
	// Time callback for back to the future (ttl testing purposes)
	timeCb  func() time.Time
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
	mx   sync.RWMutex
	list []string
}

type hash struct {
	// go 1.9+ sync.Map
	hash *sync.Map
}

func MakeServer(opts *Options) (*IqDB, error) {
	db := &IqDB{
		opts:    opts,
		distmap: NewDistmap(opts.ShardCount),
		errch:   make(chan error),
	}

	db.timeCb = db.timeFunc

	return db, nil
}

func (iq *IqDB) timeFunc() time.Time {
	return time.Now()
}

// We can redefine time to force TTL expiring
func (iq *IqDB) SetTimeFunc(cb func() time.Time) {
	iq.timeCb = cb
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