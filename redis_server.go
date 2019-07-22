package iqdb

import (
	"bufio"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"time"
)

type redisServer struct {
	port  int
	cl    Client
	ln    net.Listener
	stopc chan struct{}
}

func newRedisServer(port int, cl Client) *redisServer {
	return &redisServer{
		port:  port,
		cl:    cl,
		stopc: make(chan struct{}, 1),
	}
}
func (srv *redisServer) Serve() {
	var err error

	logrus.Info("Starting Redis server ...")

	srv.ln, err = net.Listen("tcp", ":"+strconv.Itoa(srv.port))
	if err != nil {
		panic(err)
	}

	logrus.Infof("TCP server now accept connections on port %d ...", srv.port)

	for {
		conn, err := srv.ln.Accept()
		if err != nil {
			select {
			case <-srv.stopc:
				srv.ln.Close()
				return
			default:
				logrus.Error("redis error", err)

			}
		}
		go srv.handleConnection(conn)
	}
}

func (srv *redisServer) Stop() {
	srv.stopc <- struct{}{}
}

func (srv *redisServer) handleConnection(c net.Conn) {
	reader := newRedisReader(bufio.NewReader(c))
	writer := newRedisWriter(c)

	for {
		msg, err := reader.Read()

		switch msg.Type {
		case redisTypeArray:
			switch msg.Arr[0].Type {
			case redisTypeBulk:
				switch string(msg.Arr[0].Bulk) {
				case "SET":
					if len(msg.Arr) < 3 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					val := string(msg.Arr[2].Bulk)

					var ttl time.Duration
					if len(msg.Arr) == 4 {
						ttli, err := strconv.Atoi(string(msg.Arr[3].Bulk))
						ttl = time.Duration(ttli)
						if err != nil {
							err = writer.write(ErrRedisWrongTTL)
							continue
						}
					}

					err = srv.cl.Set(key, val, ttl)
					if err != nil {
						writer.write(err)
						continue
					}

					writer.write("OK")
					continue

				case "GET":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := srv.cl.Get(key)
					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(v)
					continue
				case "DEL":
					if len(msg.Arr) < 1 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					err := srv.cl.Remove(key)
					if err != nil {
						writer.write(err)
						continue
					}

					writer.write("OK")
					continue

				case "TTL":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					var ttl time.Duration
					ttli, err := strconv.Atoi(string(msg.Arr[2].Bulk))
					ttl = time.Duration(ttli)
					if err != nil {
						err = writer.write(ErrRedisWrongTTL)
						continue
					}

					err = srv.cl.TTL(key, ttl)
					if err != nil {
						writer.write(err)
						continue
					}

					writer.write("OK")
					continue

				case "HGET":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					v, err := srv.cl.HashGet(key, field)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(v)
					continue

				case "HSET":
					if len(msg.Arr) < 3 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					err := srv.cl.HashSet(key, fields...)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write("OK")
					continue

				case "HGETALL":
					if len(msg.Arr) < 1 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := srv.cl.HashGetAll(key)

					if err != nil {
						writer.write(err)
						continue
					}

					r := make([]string, len(v)*2)

					i := 0
					for k, a := range v {
						r[i] = k
						r[i+1] = a
						i += 2
					}

					writer.writeStringSlice(r)
					continue

				case "HDEL":
					if len(msg.Arr) < 3 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					field := string(msg.Arr[2].Bulk)

					err := srv.cl.HashDel(key, field)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write("OK")
					continue

				case "HKEYS":
					if len(msg.Arr) < 1 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := srv.cl.HashKeys(key)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.writeStringSlice(v)
					continue

				case "LLEN":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					v, err := srv.cl.ListLen(key)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(v)
					continue

				case "LINDEX":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					index, err := strconv.Atoi(string(msg.Arr[2].Bulk))

					if err != nil {
						writer.write(err)
						continue
					}

					v, err := srv.cl.ListIndex(key, index)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(v)
					continue

				case "LPOP":
					if len(msg.Arr) < 2 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)

					if err != nil {
						writer.write(err)
						continue
					}

					v, err := srv.cl.ListPop(key)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(v)
					continue

				case "LRANGE":
					if len(msg.Arr) < 4 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					from, err := strconv.Atoi(string(msg.Arr[2].Bulk))

					if err != nil {
						writer.write(err)
						continue
					}

					to, err := strconv.Atoi(string(msg.Arr[3].Bulk))

					if err != nil {
						writer.write(err)
						continue
					}

					v, err := srv.cl.ListRange(key, from, to)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.writeStringSlice(v)
					continue

				case "LPUSH":
					if len(msg.Arr) < 3 {
						err = writer.write(ErrRedisWrongArgNum)
						continue
					}

					key := string(msg.Arr[1].Bulk)
					fields := make([]string, 0)

					for _, v := range msg.Arr[2:] {
						fields = append(fields, string(v.Bulk))
					}

					i, err := srv.cl.ListPush(key, fields...)

					if err != nil {
						writer.write(err)
						continue
					}

					writer.write(i)
					continue
				}
			}
		}

		err = writer.write(ErrRedisUnknownParseError)
		if err != nil {
			logrus.Error(err)
			return
		}
	}
}
