# Ballast

A small key/value store that speaks the Redis wire protocol (RESP), written in Go.
Point any Redis client at it.

## Run

```sh
go run ./cmd/ballast                       # :6379, data in ./ballast-data
go run ./cmd/ballast -addr :7000 -dir /var/lib/ballast
```

Writes go to a write-ahead log and an in-memory memtable; when the memtable
fills it is flushed to an immutable, sorted SSTable on disk. Reads check the
memtable, then the SSTables newest-first, using a per-table bloom filter to skip
those that cannot hold the key. Data survives a restart (including `kill -9`),
and `COMPACT` merges the SSTables to reclaim space from overwritten and deleted
keys.

## Try it

```sh
redis-cli -p 6379
> PING
PONG
> SET greeting "hello"
OK
> GET greeting
"hello"
> DEL greeting
(integer) 1
```

## Commands

`PING`, `ECHO`, `SET`, `GET`, `DEL`, `COMPACT`, `QUIT`.

## Test

```sh
go test ./...
go test -race ./...
```
