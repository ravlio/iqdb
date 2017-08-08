package iqdb

import (
	"net"
	"strconv"
	"github.com/ravlio/iqdb/redis"
	log "github.com/sirupsen/logrus"
	"bufio"
	"errors"
)

const ErrKeyNotFound = errors.New("key not found")

type Options struct {
	TCPPort  int
	HTTPPort int
}
type IqDB struct {
	ln     net.Listener
	reader *redis.Reader
	writer *redis.Writer
	opts   *Options
	errch  chan error
}

func MakeServer(opts *Options) (*IqDB, error) {
	db := &IqDB{
		opts:  opts,
		errch: make(chan error),
	}

	return db, nil
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

//println(string(msg.Arr[0].Bulk), string(msg.Arr[1].Bulk))
