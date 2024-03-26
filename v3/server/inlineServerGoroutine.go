package server

import (
	"fmt"
	"github.com/eopenio/itask/v3/ierrors"
	"github.com/eopenio/itask/v3/message"
	"sync"
	"time"
)

// GetNextMessageGoroutine
// describe: get next message if worker is ready
func (t *InlineServer) GetNextMessageGoroutine() {
	t.logger.DebugWithField("goroutine get_next_message start", "server", t.groupName)
	var msg message.Message
	var err error

	for range t.workerReadyChan {
		if t.IsStop() {
			break
		}
		msg, err = t.Next(t.groupName)
		if err != nil {
			go t.MakeWorkerReady()
			if !ierrors.IsEqual(err, ierrors.ErrTypeEmptyQueue) {
				t.logger.ErrorWithField(fmt.Sprint("goroutine get_next_message get msg error, ", err), "server", t.groupName)
			}
			continue
		}
		t.logger.InfoWithField(fmt.Sprintf("goroutine get_next_message new msg %+v", msg), "server", t.groupName)
		t.msgChan <- msg
	}

	t.getMessageGoroutineStopChan <- struct{}{}
	t.logger.DebugWithField("goroutine get_next_message stop", "server", t.groupName)
}

// WorkerGoroutine
// describe: start worker to run
func (t *InlineServer) WorkerGoroutine() {
	t.logger.DebugWithField("goroutine worker start", "server", t.groupName)
	waitWorkerWG := sync.WaitGroup{}
	for msg := range t.msgChan {
		go func(msg message.Message) {
			defer func() {
				e := recover()
				if e != nil {
					t.logger.ErrorWithField(fmt.Sprintf("goroutine worker run worker[%s] panic %v", msg.WorkerName, e), "server", t.groupName)
				}
			}()

			defer func() { go t.MakeWorkerReady() }()

			w, ok := t.workerMap[msg.WorkerName]
			if !ok {
				t.logger.ErrorWithField(fmt.Sprintf("goroutine worker not found worker [%s]", msg.WorkerName), "server", t.groupName)
				return
			}

			waitWorkerWG.Add(1)
			defer waitWorkerWG.Done()

			result := message.NewResult(msg.Id)
			t.workerGoroutine_RunWorker(w, &msg, &result)

		}(msg)
	}

	waitWorkerWG.Wait()
	t.workerGoroutineStopChan <- struct{}{}
	t.logger.DebugWithField("goroutine worker stop", "server", t.groupName)
}

// workerGoroutine_UpdateWorkflowResult
// return : current Workflow index
func (t *InlineServer) workerGoroutine_UpdateWorkflowResult(ctl TaskCtl, result *message.Result) int {
	workflowIndex := 0
	result.Workflow = make([][2]string, len(ctl.MsgArgs.Workflow))
	for i, w := range ctl.MsgArgs.Workflow {
		if w.WorkerName == ctl.WorkerName {
			workflowIndex = i
		}
		result.Workflow[i] = [2]string{w.WorkerName, message.WorkflowStatus.Waiting}
	}
	for i := 0; i < workflowIndex; i++ {
		result.Workflow[i][1] = message.WorkflowStatus.Success
	}
	return workflowIndex
}

// workerGoroutine_RunWorker
func (t *InlineServer) workerGoroutine_RunWorker(w WorkerInterface, msg *message.Message, result *message.Result) {
	var err error
	ctl := NewTaskCtl(*msg)
	ctl.SetServerUtil(&t.ServerUtils)
	workflowIndex := -1
	if len(ctl.MsgArgs.Workflow) > 0 {
		workflowIndex = t.workerGoroutine_UpdateWorkflowResult(ctl, result)
	}

RUN:
	if f, _ := ctl.IsAbort(); f {
		t.workerGoroutine_UpdateResultStatus(message.ResultStatus.Abort, workflowIndex, result)
		t.workerGoroutine_SaveResult(*result)
		goto AFTER
	}

	if ctl.IsExpired() {
		t.workerGoroutine_UpdateResultStatus(message.ResultStatus.Expired, workflowIndex, result)
		t.workerGoroutine_SaveResult(*result)
		goto AFTER
	}

	result.SetStatusRunning()
	t.workerGoroutine_UpdateResultStatus(result.Status, workflowIndex, result)
	t.workerGoroutine_SaveResult(*result)

	err = w.Run(&ctl, msg.FuncArgs, result)

	if err == nil {
		t.workerGoroutine_UpdateResultStatus(message.ResultStatus.Success, workflowIndex, result)
		t.workerGoroutine_SaveResult(*result)
		goto AFTER
	}
	t.logger.ErrorWithField(fmt.Sprintf("goroutine worker run worker[%s] error %s", msg.WorkerName, err), "server", t.groupName)

	if ctl.CanRetry() {
		result.Status = message.ResultStatus.WaitingRetry
		ctl.MsgArgs.RetryCount -= 1
		//msg.Ctl = ctl
		//log.TaskLog.WithField("server", t.groupName).WithField("goroutine", "worker").Infof("retry task %s", msg)
		t.logger.InfoWithField(fmt.Sprintf("goroutine worker retry task %s", msg), "server", t.groupName)
		ctl.SetError(nil)

		goto RUN
	} else {
		result.Err = err.Error()
		status := message.ResultStatus.Failure
		if ierrors.IsEqual(err, ierrors.ErrTypeAbortTask) {
			status = message.ResultStatus.Abort
		}
		t.workerGoroutine_UpdateResultStatus(status, workflowIndex, result)
		t.workerGoroutine_SaveResult(*result)
	}

AFTER:
	// 为了逻辑更简单，工作流和回调暂不兼容
	if workflowIndex >= 0 {
		if !result.IsFailure() && workflowIndex+1 < len(ctl.MsgArgs.Workflow) {
			t.workerGoroutine_NextWorkflow(workflowIndex+1, ctl, *result)
		}
	} else {
		err = w.After(&ctl, msg.FuncArgs, result)
		if err != nil {
			t.logger.ErrorWithField(fmt.Sprintf("goroutine worker run worker[%s] callback error %s", msg.WorkerName, err), "server", t.groupName)
		}
	}
}

// workerGoroutine_UpdateResultStatus
func (t *InlineServer) workerGoroutine_UpdateResultStatus(status int, workflowIndex int, result *message.Result) {
	if workflowIndex >= 0 {
		result.Workflow[workflowIndex][1] = message.StatusToWorkflowStatus[status]
		// 还有剩余任务时，result.Status不能设为Success
		if workflowIndex+1 < len(result.Workflow) && status == message.ResultStatus.Success {
			return
		}
	}
	result.Status = status
}

// workerGoroutine_SaveResult
func (t *InlineServer) workerGoroutine_SaveResult(result message.Result) {
	//log.TaskLog.WithField("server", t.groupName).WithField("goroutine", "worker").Debugf("save result %+v", result)
	t.logger.DebugWithField(fmt.Sprintf("goroutine worker save result %+v", result), "server", t.groupName)

	err := t.SetResult(result)
	if err != nil {
		//log.TaskLog.WithField("server", t.groupName).WithField("goroutine", "worker").Errorf("save result error: ", err)
		t.logger.ErrorWithField(fmt.Sprint("goroutine worker save result error: ", err), "server", t.groupName)
	}
}

// workerGoroutine_NextWorkflow
func (t *InlineServer) workerGoroutine_NextWorkflow(nextIndex int, ctl TaskCtl, result message.Result) {

	next := ctl.MsgArgs.Workflow[nextIndex]
	t.logger.DebugWithField(fmt.Sprintf("goroutine worker send next workflow [id=%s, next=%s]", ctl.Id, next.WorkerName), "server", t.groupName)

	ctl.FuncArgs = result.FuncReturn
	ctl.SetRetryCount(next.RetryCount)
	if next.RunAfter != 0 {
		n := time.Now()
		ctl.SetRunTime(n.Add(next.RunAfter))
	}
	if !next.ExpireTime.IsZero() {
		ctl.SetExpireTime(next.ExpireTime)
	}
	groupName := next.GroupName
	if !ctl.IsZeroRunTime() {
		groupName = t.GetDelayGroupName(groupName)
	}
	ctl.WorkerName = next.WorkerName
	err := t.SendMsg(groupName, ctl.Message)

	if err != nil {
		t.logger.ErrorWithField(fmt.Sprintf("send next workflow error %s [id=%s]", err, ctl.Id), "server", t.groupName)
		result.Err = ierrors.ErrSendMsg{Msg: err.Error()}.Error()
		t.workerGoroutine_UpdateResultStatus(message.ResultStatus.Failure, nextIndex, &result)
	} else {
		t.workerGoroutine_UpdateResultStatus(message.ResultStatus.Sent, nextIndex, &result)
	}

	t.workerGoroutine_SaveResult(result)

}
