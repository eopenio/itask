package brokers

import (
	"github.com/eopenio/itask/v3/message"
)

type BrokerInterface interface {
	Next(queueName string) (message.Message, error)
	Send(queueName string, msg message.Message) error
	LSend(queueName string, msg message.Message) error
	Activate() // 如果使用连接池，调用Activate后才真正建立连接
	SetPoolSize(int)
	GetPoolSize() int
	Clone() BrokerInterface
}
