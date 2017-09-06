package redis

import (
	"errors"
	"github.com/ravlio/iqdb"
	"net"
	"bufio"
	"time"
	"strconv"
	log "github.com/sirupsen/logrus"
)

type MessageType string

var ErrWrongArgNum = errors.New("Wrong arguments number")
var ErrWrongTTL = errors.New("Wrong TTL")
var ErrUnknownParseError = errors.New("Unknown parse error")

const (
	TypeString  = "+"
	TypeError   = "-"
	TypeInteger = ":"
	TypeArray   = "*"
	TypeBulk    = "$"
)

// Redis protocol format
type Message struct {
	Type   string
	String string
	Int    int64
	Arr    []*Message
	Bulk   []byte
	Err    error
}

type Redis struct {
	iq    *iqdb.IqDB
	ln    net.Listener
	stopc chan struct{}
}


func NewClient(addr string) (iqdb.Client, error) {
	c, err := net.Dial("client", addr)
	if err != nil {
		return nil, err
	}

	cl := &client{
		r: NewReader(bufio.NewReader(c)),
		w: NewWriter(c),
	}

	return cl, nil
}

func NewServer(iq *iqdb.IqDB) *Redis {
	return &Redis{
		iq:    iq,
		stopc: make(chan struct{}, 1),
	}
}

func (r *Redis) Start() error {
	var err error

	log.Info("Starting TCP server ...")

	r.ln, err = net.Listen("client", ":"+strconv.Itoa(r.iq.Opts.RedisPort))
	if err != nil {
		r.iq.Errch <- err
	}

	log.Infof("TCP server now accept connections on port %d ...", r.iq.Opts.RedisPort)

	for {
		conn, err := r.ln.Accept()
		if err != nil {
			select {
			case <-r.stopc:
				r.ln.Close()
				return nil
			default:
				log.Error("client error", err)

			}
		}
		go r.handleConnection(conn)
	}
}

func (r *Redis) Stop() {
	r.stopc <- struct{}{}

}

func (r *Redis) handleConnection(c net.Conn) {
	reader := NewReader(bufio.NewReader(c))
	writer := NewWriter(c)

	for {
		msg, err := reader.Read()

		switch msg.Type {
		case TypeArray:
			switch msg.Arr[0].Type {
			case TypeBulk:
				switch string(msg.Arr[0].Bulk) {
				case "SET":
					if len(msg.Arr) < 3 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					val := string(msg.Arr[2].Bulk)

					var ttl time.Duration
					if len(msg.Arr) == 4 {
						ttli, err := strconv.Atoi(string(msg.Arr[3].Bulk))
						ttl = time.Duration(ttli)
						if err != nil {
							err = writer.Write(ErrWrongTTL)
							continue
						}
					}

					err = r.iq.Set(key, val, ttl)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "GET":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := r.iq.Get(key)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue
				case "DEL":
					if len(msg.Arr) < 1 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					err := r.iq.Remove(key)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "TTL":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					var ttl time.Duration
					ttli, err := strconv.Atoi(string(msg.Arr[2].Bulk))
					ttl = time.Duration(ttli)
					if err != nil {
						err = writer.Write(ErrWrongTTL)
						continue
					}

					err = r.iq.TTL(key, ttl)
					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HGET":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					v, err := r.iq.HashGet(key, field)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "HSET":
					if len(msg.Arr) < 3 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					err := r.iq.HashSet(key, fields...)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HGETALL":
					if len(msg.Arr) < 1 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := r.iq.HashGetAll(key)

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
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					err := r.iq.HashDel(key, field)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write("OK")
					continue

				case "HKEYS":
					if len(msg.Arr) < 1 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := r.iq.HashKeys(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.WriteStringSlice(v)
					continue

				case "LLEN":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := r.iq.ListLen(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LINDEX":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					index, err := strconv.Atoi(string(msg.Arr[2].Bulk))

					if err != nil {
						writer.Write(err)
						continue
					}

					v, err := r.iq.ListIndex(key, index)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LPOP":
					if len(msg.Arr) < 2 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					if err != nil {
						writer.Write(err)
						continue
					}

					v, err := r.iq.ListPop(key)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(v)
					continue

				case "LRANGE":
					if len(msg.Arr) < 4 {
						err = writer.Write(ErrWrongArgNum)
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

					v, err := r.iq.ListRange(key, from, to)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.WriteStringSlice(v)
					continue

				case "LPUSH":
					if len(msg.Arr) < 3 {
						err = writer.Write(ErrWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					i, err := r.iq.ListPush(key, fields...)

					if err != nil {
						writer.Write(err)
						continue
					}

					writer.Write(i)
					continue
				}
			}
		}

		err = writer.Write(ErrUnknownParseError)
		if err != nil {
			log.Error(err)
			return
		}
	}
}
