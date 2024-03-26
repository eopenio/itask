package backends

import "github.com/eopenio/itask/v3/message"

type BackendInterface interface {
	SetResult(result message.Result, exTime int) error
	GetResult(key string) (message.Result, error)
	Activate() // 如果使用连接池，调用Activate后才真正建立连接
	SetPoolSize(int)
	GetPoolSize() int
	Clone() BackendInterface
}
