package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"runtime"
	"strings"
)

var TaskLog *logrus.Logger

type TaskHook struct {
}

func (hook TaskHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook TaskHook) Fire(entry *logrus.Entry) error {
	serverName, ok := entry.Data["server"]
	s := ""
	if ok {
		s = fmt.Sprintf("server[%s", serverName)

	}

	goroutineName, ok := entry.Data["goroutine"]
	if ok {
		goroutineName = fmt.Sprintf("|%s", goroutineName)

	}
	if !ok {
		goroutineName = ""
	}
	delete(entry.Data, "goroutine")
	delete(entry.Data, "server")

	entry.Message = fmt.Sprintf("%s%s]: %s", s, goroutineName, entry.Message)
	return nil
}

// 记录行号的hook
type LineNumHook struct {
}

func (hook LineNumHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (hook LineNumHook) Fire(entry *logrus.Entry) error {
	pc, file, line, ok := runtime.Caller(7)
	if ok {
		i := strings.Index(file, "Task")
		if i == -1 {
			return nil
		}
		entry.Data["file"] = fmt.Sprintf("%s:%d", file[i:], line)

		fu := runtime.FuncForPC(pc - 1)
		name := fu.Name()
		i = strings.LastIndex(name, "/")
		if i == -1 {
			return nil
		}
		entry.Data["func"] = name[i+1:]

	}
	return nil
}

func init() {

	TaskLog = logrus.New()
	TaskLog.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})

	TaskLog.SetLevel(logrus.InfoLevel)
	TaskLog.AddHook(&LineNumHook{})
	TaskLog.AddHook(&TaskHook{})

}

type LoggerInterface interface {
	Debug(string)
	DebugWithField(string, string, interface{})
	Info(string)
	InfoWithField(string, string, interface{})
	Warn(string)
	WarnWithField(string, string, interface{})
	Error(string)
	ErrorWithField(string, string, interface{})
	Fatal(string)
	FatalWithField(string, string, interface{})
	Panic(string)
	PanicWithField(string, string, interface{})
	SetLevel(string)
	Clone() LoggerInterface
}

type TaskLogger struct {
	logger *logrus.Logger
}

func NewTaskLogger(logger *logrus.Logger) *TaskLogger {
	return &TaskLogger{
		logger: logger,
	}
}

func (yl *TaskLogger) Debug(msg string) {
	yl.logger.Debug(msg)
}

func (yl *TaskLogger) DebugWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Debug(msg)
}

func (yl *TaskLogger) Info(msg string) {
	yl.logger.Info(msg)
}

func (yl *TaskLogger) InfoWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Info(msg)
}

func (yl *TaskLogger) Warn(msg string) {
	yl.logger.Warn(msg)
}

func (yl *TaskLogger) WarnWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Warn(msg)
}

func (yl *TaskLogger) Error(msg string) {
	yl.logger.Error(msg)
}

func (yl *TaskLogger) ErrorWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Error(msg)
}

func (yl *TaskLogger) Fatal(msg string) {
	yl.logger.Fatal(msg)
}

func (yl *TaskLogger) FatalWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Fatal(msg)
}

func (yl *TaskLogger) Panic(msg string) {
	yl.logger.Panic(msg)
}

func (yl *TaskLogger) PanicWithField(msg string, key string, val interface{}) {
	yl.logger.WithField(key, val).Panic(msg)
}

func (yl *TaskLogger) SetLevel(level string) {
	switch level {
	case "debug":
		yl.logger.SetLevel(logrus.DebugLevel)
	case "info":
		yl.logger.SetLevel(logrus.InfoLevel)
	case "warn":
		yl.logger.SetLevel(logrus.WarnLevel)
	case "error":
		yl.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		yl.logger.SetLevel(logrus.FatalLevel)
	case "panic":
		yl.logger.SetLevel(logrus.PanicLevel)
	default:
		yl.logger.SetLevel(logrus.InfoLevel)
	}
}

func (yl *TaskLogger) Clone() LoggerInterface {
	return yl
}
