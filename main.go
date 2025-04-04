package main

import (
	"log"
	"os"

	_ "github.com/lib/pq" // Controlador de PostgreSQL
	"github.com/stellviaproject/dbmap/pgsync"
	"github.com/stellviaproject/dbmap/pgutil"
)

func main() {
	config := Config{}
	if _, err := os.Stat("./config.json"); err != nil {
		config.Example()
		if err := config.Save("./config.json"); err != nil {
			log.Fatalln(err)
		}
	} else {
		if err := config.Load("./config.json"); err != nil {
			log.Fatalln(err)
		}
	}
	src, err := config.SourceDB.Connect()
	if err != nil {
		log.Fatalln(err)
	}
	pg, err := config.DestinyDB.PgConnect()
	if err != nil {
		log.Fatalln(err)
	}
	if !config.DestinyDB.HasDB(pg) {
		if err := config.DestinyDB.CreateDB(pg); err != nil {
			log.Fatalln(err)
		}
	}
	dst, err := config.DestinyDB.Connect()
	if err != nil {
		log.Fatalln(err)
	}
	info, err := pgutil.GetDataBaseInfo(src)
	if err != nil {
		log.Fatalln(err)
	}
	if err := pgsync.SyncTables(src, dst, config.Tables, info); err != nil {
		log.Fatalln(err)
	}
}
