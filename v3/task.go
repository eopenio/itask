package Task

import (
	"github.com/eopenio/itask/v3/backends"
	"github.com/eopenio/itask/v3/brokers"
	"github.com/eopenio/itask/v3/config"
	"github.com/eopenio/itask/v3/log"
	"github.com/eopenio/itask/v3/server"
)

var (
	Server = iServer{}
	Logger = iLogger{}
	Config = iConfig{}
)

type iServer struct{}

func (i iServer) NewServer(setConfigFunc ...config.SetConfigFunc) server.Server {
	c := config.NewConfig(setConfigFunc...)
	return server.NewServer(c)
}

type iConfig struct {
}

func (i iConfig) Broker(b brokers.BrokerInterface) config.SetConfigFunc {
	return config.Broker(b)
}

func (i iConfig) Backend(b backends.BackendInterface) config.SetConfigFunc {
	return config.Backend(b)
}

func (i iConfig) Debug(debug bool) config.SetConfigFunc {
	return config.Debug(debug)
}

func (i iConfig) EnableDelayServer(enable bool) config.SetConfigFunc {
	return config.EnableDelayServer(enable)
}

func (i iConfig) DelayServerQueueSize(size int) config.SetConfigFunc {
	return config.DelayServerQueueSize(size)
}

// StatusExpires default: 1 day
// task status expires in ex seconds, -1:forever,
func (i iConfig) StatusExpires(expireTime int) config.SetConfigFunc {
	return config.StatusExpires(expireTime)
}

// ResultExpires default: 1day
// task result expires in ex seconds, -1:forever,
func (i iConfig) ResultExpires(expireTime int) config.SetConfigFunc {
	return config.ResultExpires(expireTime)
}

type iLogger struct{}

func (i iLogger) NewTaskLogger() log.LoggerInterface {
	return log.NewTaskLogger(log.TaskLog)
}
