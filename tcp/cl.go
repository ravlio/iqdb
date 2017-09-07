package tcp

import (
	"github.com/ravlio/iqdb"
	"net"
	"gopkg.in/fatih/pool.v2"
	"time"
	"github.com/ravlio/iqdb/protocol"
)

type cl struct {
	p pool.Pool
}

func NewClient(addr string) (iqdb.Client, error) {
	tcp := &cl{
	}

	factory := func() (net.Conn, error) {
		return net.Dial("tcp", addr)
	}

	p, err := pool.NewChannelPool(5, 30, factory)

	if err != nil {
		return nil, err
	}

	tcp.p = p

	return tcp, nil
}

func (c *cl) Get(key string) (string, error) {
	conn, err := c.p.Get()

	if err != nil {
		return "", err
	}

	err = protocol.WriteGet(conn, key)

	if err != nil {
		return "", err
	}

	return protocol.ReadGet(conn)
}

func (c *cl) Set(key, value string, ttl ...time.Duration) error {
	conn, err := c.p.Get()

	if err != nil {
		return err
	}

	if ttl != nil {
		err = protocol.WriteSet(conn, key, value, ttl[0])
	} else {
		err = protocol.WriteSet(conn, key, value, 0)
	}

	if err != nil {
		return  err
	}

	return protocol.ReadOK(conn)
}

func (c *cl) Remove(key string) error {
	panic("implement me")
}

func (c *cl) TTL(key string, ttl time.Duration) error {
	panic("implement me")
}

func (c *cl) Keys() chan<- string {
	panic("implement me")
}

func (c *cl) ListLen(key string) (int, error) {
	panic("implement me")
}

func (c *cl) ListIndex(key string, index int) (string, error) {
	panic("implement me")
}

func (c *cl) ListPush(key string, value ...string) (int, error) {
	panic("implement me")
}

func (c *cl) ListPop(key string) (int, error) {
	panic("implement me")
}

func (c *cl) ListRange(key string, from, to int) ([]string, error) {
	panic("implement me")
}

func (c *cl) HashGet(key string, field string) (string, error) {
	panic("implement me")
}

func (c *cl) HashGetAll(key string) (map[string]string, error) {
	panic("implement me")
}

func (c *cl) HashKeys(key string) ([]string, error) {
	panic("implement me")
}

func (c *cl) HashDel(key string, field string) error {
	panic("implement me")
}

func (c *cl) HashSet(key string, args ...string) error {
	panic("implement me")
}
