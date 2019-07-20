# iqdb

fast Key-Value database with unified client interface. Can by used as embedded storage or as standalone TCP/HTTP server.

Documentation: https://godoc.org/github.com/ravlio/iqdb

## Binary protocol

Protocol is stupid simple. First byte is operation. For string format is int64 size header and then comes byte sequence. For list is additional item count. 

for string:
```
[byte Operation][int64 len][[]byte string]
```

for list:
```
[byte Operation][int64 list len][int64 item len][[]byte string]
```

example SET operation (operation, key with length, ttl (int64), value with length)

```
[1][7][testkey][10][9][testvalue]
```

error handling and empty answers are operations too. 

## Docker run on redis protocol

`docker run --rm -d -p 7379:7379 ravlio/iqdb:0.1.0`

then you can connect via
`redis-cli -p 7379`

Please use only `capital` letters for commands. E.g. `SET a 1` is allowed, `set a 1` is not allowed.
