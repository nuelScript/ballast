package main

import (
	"flag"
	"log"

	"github.com/nuelScript/ballast/internal/bitcask"
	"github.com/nuelScript/ballast/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	dir := flag.String("dir", "ballast-data", "data directory for the storage engine")
	flag.Parse()

	db, err := bitcask.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := server.New(*addr, db).ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
