package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/machinery/backends/iface"
	common2 "github.com/oarkflow/machinery/common"
	"github.com/oarkflow/machinery/config"
	"github.com/oarkflow/machinery/log"
	tasks2 "github.com/oarkflow/machinery/tasks"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
)

// Backend represents a Redis result backend
type Backend struct {
	common2.Backend
	host     string
	password string
	db       int
	pool     *redis.Pool
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	redisOnce  sync.Once
	common2.RedisConnector
}

// New creates Backend instance
func New(cnf *config.Config, host, password, socketPath string, db int) iface.Backend {
	return &Backend{
		Backend:    common2.NewBackend(cnf),
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	groupMeta := &tasks2.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
	}

	encoded, err := json.Marshal(groupMeta)
	if err != nil {
		return err
	}

	conn := b.open()
	defer conn.Close()

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", groupUUID, encoded, "EX", expiration)
	if err != nil {
		return err
	}

	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	conn := b.open()
	defer conn.Close()

	groupMeta, err := b.getGroupMeta(conn, groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(conn, groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks2.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	groupMeta, err := b.getGroupMeta(conn, groupUUID)
	if err != nil {
		return []*tasks2.TaskState{}, err
	}

	return b.getStates(conn, groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	conn := b.open()
	defer conn.Close()

	m := b.redsync.NewMutex("TriggerChordMutex")
	if err := m.Lock(); err != nil {
		return false, err
	}
	defer m.Unlock()

	groupMeta, err := b.getGroupMeta(conn, groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// Set flag to true
	groupMeta.ChordTriggered = true

	// Update the group meta
	encoded, err := json.Marshal(&groupMeta)
	if err != nil {
		return false, err
	}

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", groupUUID, encoded, "EX", expiration)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (b *Backend) mergeNewTaskState(conn redis.Conn, newState *tasks2.TaskState) {
	state, err := b.getState(conn, newState.TaskUUID)
	if err == nil {
		newState.CreatedAt = state.CreatedAt
		newState.TaskName = state.TaskName
	}
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks2.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewPendingTaskState(signature)
	return b.updateState(conn, taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks2.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewReceivedTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks2.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewStartedTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks2.Signature) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewRetryTaskState(signature)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks2.Signature, results []*tasks2.TaskResult) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewSuccessTaskState(signature, results)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks2.Signature, err string) error {
	conn := b.open()
	defer conn.Close()

	taskState := tasks2.NewFailureTaskState(signature, err)
	b.mergeNewTaskState(conn, taskState)
	return b.updateState(conn, taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks2.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	return b.getState(conn, taskUUID)
}

func (b *Backend) getState(conn redis.Conn, taskUUID string) (*tasks2.TaskState, error) {
	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}
	state := new(tasks2.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", groupUUID)
	if err != nil {
		return err
	}

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(conn redis.Conn, groupUUID string) (*tasks2.GroupMeta, error) {

	item, err := redis.Bytes(conn.Do("GET", groupUUID))
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks2.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(conn redis.Conn, taskUUIDs ...string) ([]*tasks2.TaskState, error) {
	taskStates := make([]*tasks2.TaskState, len(taskUUIDs))

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		taskUUIDInterfaces[i] = interface{}(taskUUID)
	}

	reply, err := redis.Values(conn.Do("MGET", taskUUIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("Expected byte array, instead got: %v", value)
		}

		taskState := new(tasks2.TaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			log.ERROR.Print(err)
			return taskStates, err
		}

		taskStates[i] = taskState
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(conn redis.Conn, taskState *tasks2.TaskState) error {
	encoded, err := json.Marshal(taskState)
	if err != nil {
		return err
	}

	expiration := int64(b.getExpiration().Seconds())
	_, err = conn.Do("SET", taskState.TaskUUID, encoded, "EX", expiration)
	if err != nil {
		return err
	}

	return nil
}

// getExpiration returns expiration for a stored task state
func (b *Backend) getExpiration() time.Duration {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}

	return time.Duration(expiresIn) * time.Second
}

// open returns or creates instance of Redis connection
func (b *Backend) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New(redsyncredis.NewPool(b.pool))
	})
	return b.pool.Get()
}
