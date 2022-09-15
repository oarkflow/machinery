package mongo_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/backends/mongo"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
)

var (
	groupUUID = "123456"
	taskUUIDs = []string{"1", "2", "3"}
)

func newBackend() (iface.Backend, error) {
	cnf := &config.Config{
		ResultBackend:   os.Getenv("MONGODB_URL"),
		ResultsExpireIn: 30,
	}
	backend, err := mongo.New(cnf)
	if err != nil {
		return nil, err
	}

	backend.PurgeGroupMeta(groupUUID)
	for _, taskUUID := range taskUUIDs {
		backend.PurgeState(taskUUID)
	}

	if err := backend.InitGroup(groupUUID, taskUUIDs); err != nil {
		return nil, err
	}
	return backend, nil
}

func TestNew(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if assert.NoError(t, err) {
		assert.NotNil(t, backend)
	}
}

func TestSetStatePending(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	err = backend.SetStatePending(&tasks.Signature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, tasks.StatePending, taskState.State, "Not StatePending")
		}
	}
}

func TestSetStateReceived(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	err = backend.SetStateReceived(&tasks.Signature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, tasks.StateReceived, taskState.State, "Not StateReceived")
		}
	}
}

func TestSaveStatePending(t *testing.T) {
	cnf := &config.Config{
		ResultBackend:   "mongodb://config:config@hkg-test-mgo.everonet.com:23636,hkg-test-mgo.everonet.com:23637,hkg-test-mgo.everonet.com:23638/config",
		ResultsExpireIn: 30,
		MongoDB: &config.MongoDBConfig{
			Database:        "config",
			NoticeTasksColl: "evo.pigeon.noticeTasks",
		},
	}
	backend, err := mongo.NewMongoBackEnd(cnf)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	sig := &tasks.Signature{
		UUID: "1232131231",
		Name: "notification",
		ETA:  &now,
	}
	ns := &tasks.NotificationSignature{
		Signature: sig,
		//EvoTransID: "12313",
	}
	err = backend.SaveStatePending(ns)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetTaskByEvoTransID(t *testing.T) {
	cnf := &config.Config{
		ResultBackend:   "mongodb://config:config@hkg-test-mgo.everonet.com:23636,hkg-test-mgo.everonet.com:23637,hkg-test-mgo.everonet.com:23638/config",
		ResultsExpireIn: 30,
		MongoDB: &config.MongoDBConfig{
			Database:        "config",
			NoticeTasksColl: "evo.pigeon.noticeTasks",
		},
	}
	backend, err := mongo.NewMongoBackEnd(cnf)
	if err != nil {
		t.Fatal(err)
	}

	task, err := backend.GetTaskByEvoTransID("1b7b6b7fcd0140a0ad43c427b66b15da")
	if err != nil {
		t.Error(err)
	}
	t.Logf("%+v", task)
}

func TestSetStateStarted(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	err = backend.SetStateStarted(&tasks.Signature{
		UUID: taskUUIDs[0],
	})
	if assert.NoError(t, err) {
		taskState, err := backend.GetState(taskUUIDs[0])
		if assert.NoError(t, err) {
			assert.Equal(t, tasks.StateStarted, taskState.State, "Not StateStarted")
		}
	}
}

func TestSetStateSuccess(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	resultType := "float64"
	resultValue := float64(88.5)

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	signature := &tasks.Signature{
		UUID: taskUUIDs[0],
	}
	taskResults := []*tasks.TaskResult{
		{
			Type:  resultType,
			Value: resultValue,
		},
	}
	err = backend.SetStateSuccess(signature, taskResults)
	assert.NoError(t, err)

	taskState, err := backend.GetState(taskUUIDs[0])
	assert.NoError(t, err)
	assert.Equal(t, tasks.StateSuccess, taskState.State, "Not StateSuccess")
	assert.Equal(t, resultType, taskState.Results[0].Type, "Wrong result type")
	assert.Equal(t, float64(resultValue), taskState.Results[0].Value.(float64), "Wrong result value")
}

func TestSetStateFailure(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	failString := "Fail is ok"

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	signature := &tasks.Signature{
		UUID: taskUUIDs[0],
	}
	err = backend.SetStateFailure(signature, failString)
	assert.NoError(t, err)

	taskState, err := backend.GetState(taskUUIDs[0])
	assert.NoError(t, err)
	assert.Equal(t, tasks.StateFailure, taskState.State, "Not StateSuccess")
	assert.Equal(t, failString, taskState.Error, "Wrong fail error")
}

func TestGroupCompleted(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}
	taskResultsState := make(map[string]string)

	isCompleted, err := backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if assert.NoError(t, err) {
		assert.False(t, isCompleted, "Actually group is not completed")
	}

	signature := &tasks.Signature{
		UUID: taskUUIDs[0],
	}
	err = backend.SetStateFailure(signature, "Fail is ok")
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[0]] = tasks.StateFailure

	signature = &tasks.Signature{
		UUID: taskUUIDs[1],
	}
	taskResults := []*tasks.TaskResult{
		{
			Type:  "string",
			Value: "Result ok",
		},
	}
	err = backend.SetStateSuccess(signature, taskResults)
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[1]] = tasks.StateSuccess

	signature = &tasks.Signature{
		UUID: taskUUIDs[2],
	}
	err = backend.SetStateSuccess(signature, taskResults)
	assert.NoError(t, err)
	taskResultsState[taskUUIDs[2]] = tasks.StateSuccess

	isCompleted, err = backend.GroupCompleted(groupUUID, len(taskUUIDs))
	if assert.NoError(t, err) {
		assert.True(t, isCompleted, "Actually group is completed")
	}

	taskStates, err := backend.GroupTaskStates(groupUUID, len(taskUUIDs))
	assert.NoError(t, err)

	assert.Equal(t, len(taskStates), len(taskUUIDs), "Wrong len tasksStates")
	for _, taskState := range taskStates {
		assert.Equal(
			t,
			taskResultsState[taskState.TaskUUID],
			taskState.State,
			"Wrong state on", taskState.TaskUUID,
		)
	}
}

func TestGroupStates(t *testing.T) {
	if os.Getenv("MONGODB_URL") == "" {
		t.Skip("MONGODB_URL is not defined")
	}

	backend, err := newBackend()
	if err != nil {
		t.Fatal(err)
	}

	taskStates, err := backend.GroupTaskStates(groupUUID, len(taskUUIDs))
	assert.NoError(t, err)
	for i, taskState := range taskStates {
		assert.Equal(t, taskUUIDs[i], taskState.TaskUUID)
	}
}
