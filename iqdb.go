package iqdb

import (
	"net"
	"strconv"
	"github.com/ravlio/iqdb/redis"
	log "github.com/sirupsen/logrus"
	"bufio"
	"errors"
)

type Options struct {
	TCPPort  *int
	HTTPPort *int
}
type iqdb struct {
	ln     net.Listener
	reader *redis.Reader
	writer *redis.Writer
	opts   *Options
	errch  chan error
}

func MakeServer(opts *Options) (*iqdb, error) {
	db := &iqdb{
		opts:  opts,
		errch: make(chan error),
	}

	return db, nil
}

func (iq *iqdb) Start() error {
	if iq.opts.TCPPort != nil {
		go iq.serveTCP()
	}

	if iq.opts.HTTPPort != nil {
		go iq.serveHTTP()
	}

	return <-iq.errch
}

func (iq *iqdb) serveHTTP() {
	log.Info("Starting HTTP server ...")

	log.Infof("HTTP server now accept connections on port %d ...", *iq.opts.HTTPPort)
}

func (iq *iqdb) serveTCP() {
	var err error

	log.Info("Starting TCP server ...")

	iq.ln, err = net.Listen("tcp", ":"+strconv.Itoa(*iq.opts.TCPPort))
	if err != nil {
		iq.errch <- err
		return
	}

	log.Infof("TCP server now accept connections on port %d ...", *iq.opts.TCPPort)

	for {
		conn, err := iq.ln.Accept()
		if err != nil {
			log.Error("tcp error", err)
		}
		go iq.handleConnection(conn)
	}
}

func (iq *iqdb) handleConnection(c net.Conn) {
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
