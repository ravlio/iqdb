# iqdb

*POC, not for production, made just for showing my code style, nothing more*

Documentation: https://godoc.org/github.com/ravlio/iqdb

IqDB is:
- Fast
- Multi-protocol in-memory database
- Supports k/v, hashes, lists
- Sync/async binary AOF-persistence 
- TTL on BTree
- Supports Redis text protocol on TCP
- Can be used in embedded mode

TODO:
- Raw TCP client/server with multiplexing
- HTTP client/server
- Replication

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
