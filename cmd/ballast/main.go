package main

import (
	"flag"
	"log"

	"github.com/nuelScript/ballast/internal/engine"
	"github.com/nuelScript/ballast/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	aofPath := flag.String("aof", "ballast.aof", "append-only log file (empty to disable persistence)")
	flag.Parse()

	eng, err := engine.Open(*aofPath)
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close()

	if err := server.New(*addr, eng).ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
