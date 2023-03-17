package null

import (
	"fmt"
	"github.com/oarkflow/machinery/backends/iface"
	"github.com/oarkflow/machinery/common"
	"github.com/oarkflow/machinery/config"
	tasks2 "github.com/oarkflow/machinery/tasks"
)

// ErrGroupNotFound ...
type ErrGroupNotFound struct {
	groupUUID string
}

// NewErrGroupNotFound returns new instance of ErrGroupNotFound
func NewErrGroupNotFound(groupUUID string) ErrGroupNotFound {
	return ErrGroupNotFound{groupUUID: groupUUID}
}

// Error implements error interface
func (e ErrGroupNotFound) Error() string {
	return fmt.Sprintf("Group not found: %v", e.groupUUID)
}

// ErrTasknotFound ...
type ErrTasknotFound struct {
	taskUUID string
}

// NewErrTasknotFound returns new instance of ErrTasknotFound
func NewErrTasknotFound(taskUUID string) ErrTasknotFound {
	return ErrTasknotFound{taskUUID: taskUUID}
}

// Error implements error interface
func (e ErrTasknotFound) Error() string {
	return fmt.Sprintf("Task not found: %v", e.taskUUID)
}

// Backend represents an "null" result backend
type Backend struct {
	common.Backend
	groups map[string]struct{}
}

// New creates NullBackend instance
func New() iface.Backend {
	return &Backend{
		Backend: common.NewBackend(new(config.Config)),
		groups:  make(map[string]struct{}),
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	b.groups[groupUUID] = struct{}{}
	return nil
}

// GroupCompleted returns true (always)
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	_, ok := b.groups[groupUUID]
	if !ok {
		return false, NewErrGroupNotFound(groupUUID)
	}

	return true, nil
}

// GroupTaskStates returns null states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks2.TaskState, error) {
	_, ok := b.groups[groupUUID]
	if !ok {
		return nil, NewErrGroupNotFound(groupUUID)
	}

	ret := make([]*tasks2.TaskState, 0, groupTaskCount)
	return ret, nil
}

// TriggerChord returns true (always)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks2.Signature) error {
	state := tasks2.NewPendingTaskState(signature)
	return b.updateState(state)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks2.Signature) error {
	state := tasks2.NewReceivedTaskState(signature)
	return b.updateState(state)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks2.Signature) error {
	state := tasks2.NewStartedTaskState(signature)
	return b.updateState(state)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks2.Signature) error {
	state := tasks2.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks2.Signature, results []*tasks2.TaskResult) error {
	state := tasks2.NewSuccessTaskState(signature, results)
	return b.updateState(state)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks2.Signature, err string) error {
	state := tasks2.NewFailureTaskState(signature, err)
	return b.updateState(state)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks2.TaskState, error) {
	return nil, NewErrTasknotFound(taskUUID)
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	return NewErrTasknotFound(taskUUID)
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	_, ok := b.groups[groupUUID]
	if !ok {
		return NewErrGroupNotFound(groupUUID)
	}

	return nil
}

func (b *Backend) updateState(s *tasks2.TaskState) error {
	return nil
}
