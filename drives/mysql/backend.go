package mysql

import (
	"github.com/eopenio/itask/v3/backends"
	"github.com/eopenio/itask/v3/message"
)

type Backend struct {
	client   *Client
	host     string
	port     string
	user     string
	password string
	db       string
	idleConn int
	idleTime int
	maxConn  int
	maxTime  int
}

func NewMySQLBackend(host, port, user, password, db string, idleConn, maxConn, idleTime, maxTime int) Backend {
	return Backend{
		host:     host,
		port:     port,
		user:     user,
		password: password,
		db:       db,
		idleConn: idleConn,
		maxConn:  maxConn,
		idleTime: idleTime,
		maxTime:  maxTime,
	}
}

func (c *Backend) Activate() {
	c.client = NewMySQLClient(c.host, c.port, c.user, c.password, c.db, c.idleConn, c.maxConn, c.idleTime, c.maxTime)
}

func (c *Backend) SetPoolSize(n int) {
}

func (c *Backend) GetPoolSize() int {
	return 0
}

func (c *Backend) SetResult(result message.Result, exTime int) error {
	err, _ := c.client.SaveMsgResult(result)
	return err
}

func (c *Backend) GetResult(key string) (message.Result, error) {
	var result message.Result
	result, _, err := c.client.TakeMsgResult(key)
	return result, err
}

func (c Backend) Clone() backends.BackendInterface {
	return &Backend{
		host:     c.host,
		port:     c.port,
		user:     c.user,
		password: c.password,
		db:       c.db,
		idleConn: c.idleConn,
		maxConn:  c.maxConn,
		idleTime: c.idleTime,
		maxTime:  c.maxTime,
	}
}
