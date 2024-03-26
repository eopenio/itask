package message

import (
	"github.com/eopenio/itask/v3/util"
	"github.com/google/uuid"
	"time"
)

type Message struct {
	Id         string      `json:"id"`
	WorkerName string      `json:"worker_name"`
	FuncArgs   []string    `json:"func_args"`
	MsgArgs    MessageArgs `v2JsonName:"TaskCtl"` // 为了方便client端send时通过SetTaskCtl修改相关参数
}

type MessageArgs struct {
	RetryCount int
	RunTime    time.Time             // 指定任务延后多长时间执行
	ExpireTime time.Time             // 指定任务过期时间
	Workflow   []MessageWorkflowArgs `json:"workflow"`
}

type MessageWorkflowArgs struct {
	GroupName  string
	WorkerName string
	RetryCount int
	RunAfter   time.Duration
	RunAt      time.Time
	ExpireTime time.Time
}

func NewMsgArgs() MessageArgs {
	return MessageArgs{RetryCount: 3}
}

func NewMessage(msgArgs MessageArgs) Message {
	id := uuid.New().String()
	return Message{
		Id:      id,
		MsgArgs: msgArgs,
	}
}

func (m *Message) SetArgs(args ...interface{}) error {
	r, err := util.GoVarsToTaskJsonSlice(args...)
	if err != nil {
		return err
	}
	m.FuncArgs = r
	return nil
}

func (m Message) IsRunTime() bool {
	n := time.Now().Unix()
	return n >= m.MsgArgs.GetRunTime().Unix()
}

func (m Message) RunTimeAfter(t time.Time) bool {
	return m.MsgArgs.GetRunTime().Unix() > t.Unix()
}

func (m Message) RunTimeAfterOrEqual(t time.Time) bool {
	return m.MsgArgs.GetRunTime().Unix() >= t.Unix()
}

func (m Message) RunTimeBefore(t time.Time) bool {
	return m.MsgArgs.GetRunTime().Unix() < t.Unix()
}

func (m Message) RunTimeBeforeOrEqual(t time.Time) bool {
	return m.MsgArgs.GetRunTime().Unix() <= t.Unix()
}

func (m Message) RunTimeEqual(t time.Time) bool {
	return m.MsgArgs.GetRunTime().Unix() == t.Unix()
}

func (m MessageArgs) IsDelayMessage() bool {
	return !m.RunTime.IsZero()
}

func (t *MessageArgs) AppendWorkflow(work MessageWorkflowArgs) {
	t.Workflow = append(t.Workflow, work)
}

func (t MessageArgs) GetRunTime() time.Time {
	return t.RunTime
}
