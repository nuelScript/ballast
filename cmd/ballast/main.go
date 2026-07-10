package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/nuelScript/ballast/internal/lsm"
	"github.com/nuelScript/ballast/internal/raft"
	"github.com/nuelScript/ballast/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "client (RESP) address")
	dir := flag.String("dir", "ballast-data", "data directory")
	raftAddr := flag.String("raft", "", "this node's Raft address (empty = standalone)")
	cluster := flag.String("cluster", "", "comma-separated raftAddr=clientAddr for every node")
	flag.Parse()

	db, err := lsm.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *raftAddr == "" {
		if err := server.New(*addr, db).ListenAndServe(); err != nil {
			log.Fatal(err)
		}
		return
	}

	var peers []string
	redirect := make(map[string]string)
	for _, entry := range strings.Split(*cluster, ",") {
		rAddr, cAddr, ok := strings.Cut(entry, "=")
		if !ok {
			log.Fatalf("bad -cluster entry %q, want raftAddr=clientAddr", entry)
		}
		redirect[rAddr] = cAddr
		if rAddr != *raftAddr {
			peers = append(peers, rAddr)
		}
	}

	srv, node := server.NewCluster(*addr, db, raft.Config{
		ID:        *raftAddr,
		Peers:     peers,
		Transport: raft.NewHTTPTransport(),
		Storage:   raft.NewFileStorage(filepath.Join(*dir, "raft-state.json")),
	}, redirect)

	go func() {
		if err := http.ListenAndServe(*raftAddr, node.Handler()); err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("ballast node: raft=%s client=%s peers=%v", *raftAddr, *addr, peers)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
