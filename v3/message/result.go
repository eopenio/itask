package message

import (
	"errors"
	"fmt"
	"github.com/eopenio/itask/v3/util/yjson"
	"strings"
)

type resultStatusChoice struct {
	Sent         int
	FirstRunning int
	WaitingRetry int
	Running      int
	Success      int
	Failure      int
	Expired      int
	Abort        int // 手动中止任务
}

var ResultStatus = resultStatusChoice{
	Sent:         0,
	FirstRunning: 1,
	WaitingRetry: 2,
	Running:      3,
	Success:      4,
	Failure:      5,
	Expired:      6,
	Abort:        7, // 手动中止任务
}

type workflowStatusChoice struct {
	Waiting string
	Running string
	Success string
	Failure string
	Expired string
	Abort   string
}

var WorkflowStatus = workflowStatusChoice{
	Waiting: "waiting",
	Running: "running",
	Success: "success",
	Failure: "failure",
	Expired: "expired",
	Abort:   "abort", // 手动中止任务
}

var StatusToWorkflowStatus = map[int]string{
	ResultStatus.Sent:         WorkflowStatus.Waiting,
	ResultStatus.FirstRunning: WorkflowStatus.Running,
	ResultStatus.WaitingRetry: WorkflowStatus.Running,
	ResultStatus.Running:      WorkflowStatus.Running,
	ResultStatus.Success:      WorkflowStatus.Success,
	ResultStatus.Failure:      WorkflowStatus.Failure,
	ResultStatus.Expired:      WorkflowStatus.Expired,
	ResultStatus.Abort:        WorkflowStatus.Abort,
}

type Result struct {
	Id         string      `json:"id" gorm:"column:task_id;comment:任务ID;type:varchar(50);size:50;uniqueIndex:idx_taskid;primaryKey"`
	Status     int         `json:"status" gorm:"column:status;comment:任务状态;type:int;size:10;index:idx_status"` // 0:sent , 1:first running , 2: waiting to retry , 3: running , 4: success , 5: Failure
	FuncReturn []string    `json:"func_return" gorm:"column:func_return;comment:任务返回内容;type:mediumtext;"`
	RetryCount int         `json:"retry_count" gorm:"column:retry_count;comment:任务重试次数;type:int;size:10;"`
	Workflow   [][2]string `json:"workflow" gorm:"-"` // [["workName","status"],] ;  status: waiting , running , success , failure , expired , abort
	Err        string      `json:"err" gorm:"column:error_msg;comment:错误信息;type:varchar(256);size:50;"`
}

func NewResult(id string) Result {
	return Result{
		Id: id,
	}
}

func (r Result) GetBackendKey() string {
	return "itask:backend:" + r.Id
}

func (r Result) GetIdFromKey(key string) string {
	if len(strings.Split(key, ":")) == 3 {
		return strings.Split(key, ":")[2]
	}
	return errors.New("task key is invalid").Error()
}

func (r *Result) SetStatusRunning() {
	if r.Status == ResultStatus.Sent {
		r.Status = ResultStatus.FirstRunning
	} else {
		r.Status = ResultStatus.Running
		r.RetryCount += 1
	}
}

func (r Result) Get(index int, v interface{}) error {
	err := yjson.TaskJson.UnmarshalFromString(r.FuncReturn[index], v)
	return err
}

func (r Result) Gets(args ...interface{}) error {
	for i, v := range args {
		err := yjson.TaskJson.UnmarshalFromString(r.FuncReturn[i], v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r Result) GetInt64(index int) (int64, error) {
	var v int64
	err := r.Get(index, &v)
	return v, err
}

func (r Result) GetUint64(index int) (uint64, error) {
	var v uint64
	err := r.Get(index, &v)
	return v, err
}

func (r Result) GetFloat64(index int) (float64, error) {
	var v float64
	err := r.Get(index, &v)
	return v, err
}

func (r Result) GetBool(index int) (bool, error) {
	var v bool
	err := r.Get(index, &v)
	return v, err
}

func (r Result) GetString(index int) (string, error) {
	var v string
	err := r.Get(index, &v)
	return v, err
}

func (r Result) IsSuccess() bool {
	return r.Status == ResultStatus.Success
}

func (r Result) IsFailure() bool {
	if r.Status == ResultStatus.Failure || r.Status == ResultStatus.Expired || r.Status == ResultStatus.Abort {
		return true
	}
	return false
}

func (r Result) IsFinish() bool {
	if r.Status == ResultStatus.Success || r.IsFailure() {
		return true
	}
	return false
}

// GetAbortKey 结束任务标志
func GetAbortKey(id string) string {
	return fmt.Sprintf("Abort:%s", id)

}

func NewAbortResult(id string) Result {
	return Result{
		Id: GetAbortKey(id),
	}
}
