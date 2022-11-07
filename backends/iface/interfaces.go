package iface

import (
	tasks2 "github.com/sujit-baniya/machinery/tasks"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(groupUUID string, taskUUIDs []string) error
	GroupCompleted(groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks2.TaskState, error)
	TriggerChord(groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(signature *tasks2.Signature) error
	SetStateReceived(signature *tasks2.Signature) error
	SetStateStarted(signature *tasks2.Signature) error
	SetStateRetry(signature *tasks2.Signature) error
	SetStateSuccess(signature *tasks2.Signature, results []*tasks2.TaskResult) error
	SetStateFailure(signature *tasks2.Signature, err string) error
	GetState(taskUUID string) (*tasks2.TaskState, error)

	// Purging stored stored tasks states and group meta data
	IsAMQP() bool
	PurgeState(taskUUID string) error
	PurgeGroupMeta(groupUUID string) error
}
