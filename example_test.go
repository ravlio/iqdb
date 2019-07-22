package iqdb_test

import (
	"github.com/ravlio/iqdb"
	"os"
)

func ExampleEmbeddedServer() {
	var err error

	// Cleanup test db
	if _, err := os.Stat("test"); err == nil {
		os.Remove("test")
	}

	// Open new db or use existing one
	db, err = iqdb.Open("test", &iqdb.Options{RedisPort: 7777, HTTPPort: 8888, ShardCount: 100})
	defer db.Close()

	if err != nil {
		panic(err)
	}

	// Start servers
	// Additional goroutine is needed to allow make servers and clients in same time
	go func() {
		panic(db.Start())
	}()

	if err != nil {
		panic(err)
	}
}

func ExampleRedisClient() {
	tcp, err := iqdb.NewRedisClient(":7777")

	if err != nil {
		panic(err)
	}

	err = tcp.Set("k", "v")

	if err != nil {
		panic(err)
	}
}
