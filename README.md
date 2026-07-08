# Ballast

A small key/value store that speaks the Redis wire protocol (RESP), written in Go.
Point any Redis client at it.

## Run

```sh
go run ./cmd/ballast                       # :6379, log at ./ballast.aof
go run ./cmd/ballast -addr :7000
go run ./cmd/ballast -aof ""               # in-memory, no persistence
```

Writes are appended to an on-disk log before they are acknowledged, and the log
is replayed on startup — so data survives a restart (including `kill -9`).

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

`PING`, `ECHO`, `SET`, `GET`, `DEL`, `QUIT`.

## Test

```sh
go test ./...
go test -race ./...
```
