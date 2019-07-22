package main

import "github.com/ravlio/iqdb"
import (
	"flag"
	log "github.com/sirupsen/logrus"
)

var dbname = flag.String("dbname", "db", "database filename")
var tcpPort = flag.Int("tcp", 7379, "tcp port")

func main() {
	log.Info("Starting ...")
	db, err := iqdb.Open(*dbname, &iqdb.Options{
		RedisPort: *tcpPort,
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(db.Start())
}
