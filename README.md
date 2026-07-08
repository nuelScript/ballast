# Ballast

A small key/value store that speaks the Redis wire protocol (RESP), written in Go.
Point any Redis client at it.

## Run

```sh
go run ./cmd/ballast                       # :6379, data in ./ballast-data
go run ./cmd/ballast -addr :7000 -dir /var/lib/ballast
```

Values are stored in append-only segment files on disk; only an index of
`key → file location` is kept in memory, so the dataset can outgrow RAM. Records
are CRC-checked, and data survives a restart (including `kill -9`). `COMPACT`
rewrites the live records into a fresh segment to reclaim space held by
overwritten and deleted keys.

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
