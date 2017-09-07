# iqdb

![CircleCI status](https://circleci.com/gh/ravlio/iqdb.png)

fast Key-Value database with unified client interface. Can by used as embedded storage or as standalone TCP/HTTP server.

Documentation: https://godoc.org/github.com/ravlio/iqdb

## Binary protocol

Protocol is dead simple. First byte is operation. For string format is int64 size header and then comes byte sequence. For list is additional item count. 

for string:
```
[byte Operation][int64 len][[]byte string]
```

for list:
```
[byte Operation][int64 list len][int64 item len][[]byte string]
```


## Docker run

`docker run -it -d -p 7369:7369 -v /local/path/to/db:/iqdb iqdb:0.1.0`

