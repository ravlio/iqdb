package tcp

import (
	"github.com/ravlio/iqdb"
	"net"
	"strconv"
	log "github.com/sirupsen/logrus"
	"gopkg.in/fatih/pool.v2"
	"github.com/xtaci/smux"
	"github.com/ravlio/iqdb/protocol"
	"errors"
)

var ErrReadOp = errors.New("read op error")

type Server struct {
	iq    *iqdb.IqDB
	ln    net.Listener
	pool
	stopc chan struct{}
}

type cl struct {
	p pool.Pool
}



func NewServer(iq *iqdb.IqDB) *Server {
	return &Server{
		iq:    iq,
		stopc: make(chan struct{}, 1),
	}
}

func (r *Server) Start() error {
	var err error

	log.Info("Starting TCP server ...")

	r.ln, err = net.Listen("tcp", ":"+strconv.Itoa(r.iq.Opts.RedisPort))
	if err != nil {
		r.iq.Errch <- err
	}

	log.Infof("TCP server now accept connections on port %d ...", r.iq.Opts.RedisPort)

	for {
		conn, err := r.ln.Accept()

		// Setup server side of smux
		session, err := smux.Server(conn, nil)
		if err != nil {
			panic(err)
		}

		for {
			// Accept a stream
			stream, err := session.AcceptStream()
			w := protocol.NewWriter(stream)
			if err != nil {
				select {
				case <-r.stopc:
					stream.Close()
					r.ln.Close()
					return nil
				default:
					log.Error("tcp error", err)
				}
			}

			op, err := protocol.ReadOp(stream)

			if err != nil {
				w.WriteError(ErrReadOp)
			}

			switch op {
			case protocol.OpSet:
				k, ttl, v, err := protocol.ReadSet(stream)
				if err != nil {
					w.WriteError(err)
				}

				err = r.iq.Set(k, v, ttl)
				if err != nil {
					w.WriteError(err)
				}
				err = w.WriteOK()
				if err != nil {
					w.WriteError(err)
				}
			case protocol.OpGet:
				k, err := protocol.ReadGet(stream)
				if err != nil {
					w.WriteError(err)
				}

				r, err := r.iq.Get(k)

				if err != nil {
					w.WriteError(err)
				}

				err = w.WriteGet(r)
				if err != nil {
					w.WriteError(err)
				}
			}

		}

	}
}

func (r *Server) Stop() {
	r.stopc <- struct{}{}
}
