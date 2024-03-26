# Redis 

## Installation

```shell
go get -u github.com/eopenio/itask/drives/redis/v3
```

## Broker

```go
package main

import (
    "github.com/eopenio/itask/drives/redis/v3"
)

func main() {
	broker := redis.NewRedisBroker("127.0.0.1", "6379", "", 0, 3)
	// ...
}
```


## Backend

```go
package main

import (
    "github.com/eopenio/itask/drives/redis/v3"
)

func main() {
	backend := redis.NewRedisBackend("127.0.0.1", "6379", "", 0, 10)
	// ...
}
```
