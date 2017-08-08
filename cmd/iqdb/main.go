package main

import "github.com/ravlio/iqdb"
import (
	"flag"
	log "github.com/sirupsen/logrus"
)

var tcpPort = flag.Int("tcp", 7379, "tcp port")
var httpPort = flag.Int("http", 8081, "http port")

func main() {
	log.Info("Starting ...")
	db, err := iqdb.MakeServer(&iqdb.Options{
		TCPPort:  *tcpPort,
		HTTPPort: *httpPort,
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(db.Start())
}
