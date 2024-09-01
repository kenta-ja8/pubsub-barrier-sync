package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/helper"
	"github.com/kenta-ja8/pubsub-barrier-sync/pkg/usecase"
	"github.com/pkg/errors"
)

type TaskCode string

const (
	TaskCode_A TaskCode = "TaskCode-A"
	TaskCode_B TaskCode = "TaskCode-B"
	TaskCode_C TaskCode = "TaskCode-C"
)

type CustomMessage struct {
	JobID           string                `json:"jobId"`
	TaskCode        TaskCode              `json:"taskCode"`
	TaskData        []byte                `json:"taskData"`
	BarrierJobIDMap map[TaskCode][]string `json:"barrierJobIdMap"`
	HistoryJobIDs   []string              `json:"historyJobIds"`
	TraceID         string                `json:"traceId"`
}

var pubsubDeadline = 60 * time.Second
var bufferTime = 5 * time.Second

var mux sync.Mutex
var waitJobIDMap map[TaskCode][]string = make(map[TaskCode][]string)
var ackJobIDMap map[string]func() = make(map[string]func())

func DispatchMessage(ctx context.Context, msg *pubsub.Message, topic *pubsub.Topic) error {
	var cm CustomMessage
	if err := json.Unmarshal(msg.Data, &cm); err != nil {
		msg.Nack()
		return errors.Wrap(err, "failed to unmarshal message")
	}
	log.Printf("[%s] TaskData: %+v\n", cm.TraceID, cm.TaskData)

	barrierJobIDs := cm.BarrierJobIDMap[cm.TaskCode]
	if len(barrierJobIDs) != 0 {
		log.Printf("[%s] has BarrierJobIDs\n", cm.TraceID)
		mux.Lock()
		defer mux.Unlock()
		for _, barrierJobID := range barrierJobIDs {
			for _, historyJobID := range cm.HistoryJobIDs {
				if barrierJobID == historyJobID {
					log.Println("add BarrierJobID to waitJobIDMap", barrierJobID, cm.TaskCode)
					waitJobIDMap[cm.TaskCode] = append(waitJobIDMap[cm.TaskCode], barrierJobID)
					ackJobIDMap[barrierJobID] = func() { msg.Ack() }
					go func() {
						log.Printf("[%s] start to delete BarrierJobID from waitJobIDMap\n", cm.TraceID)
						time.Sleep(pubsubDeadline - bufferTime)
						mux.Lock()
						defer mux.Unlock()
						waitJobIDs := make([]string, 0, len(waitJobIDMap[cm.TaskCode]))
						for _, barrierJobID := range waitJobIDMap[cm.TaskCode] {
							if barrierJobID != historyJobID {
								waitJobIDs = append(waitJobIDs, barrierJobID)
							} else {
								log.Printf("[%s] delete BarrierJobID from waitJobIDMap:%+v\n", cm.TraceID, barrierJobID)
								delete(ackJobIDMap, barrierJobID)
							}
						}
						waitJobIDMap[cm.TaskCode] = waitJobIDs
					}()
				}
			}
		}
		if !helper.AllElementsInSlice(barrierJobIDs, waitJobIDMap[cm.TaskCode]) {
			log.Printf("[%s] ------------------------ waiting for BarrierJobIDs\n", cm.TraceID)
			return nil
		}
		for _, barrierJobID := range barrierJobIDs {
			ackJobIDMap[barrierJobID]()
			delete(ackJobIDMap, barrierJobID)
		}
		waitJobIDMap[cm.TaskCode] = helper.RemoveElements(waitJobIDMap[cm.TaskCode], barrierJobIDs)
		log.Printf("[%s] -------------------- start barriered Jobs because all BarrierJobIDs are finished\n", cm.TraceID)
	}

	switch cm.TaskCode {
	case TaskCode_A:
		log.Println(cm.TaskCode)
		var taskDataUsecaseA usecase.TaskDataUsecaseA
		err := json.Unmarshal([]byte(cm.TaskData), &taskDataUsecaseA)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal message")
		}
		result := usecase.UsecaseA(taskDataUsecaseA)
		msg.Ack()

		nextJobIDs := make([]string, 3)
		for num := range 3 {
			nextJobIDs[num] = helper.GenerateID()
		}
		bjim := maps.Clone(cm.BarrierJobIDMap)
		if bjim == nil {
			bjim = make(map[TaskCode][]string)
		}
		bjim[TaskCode_C] = nextJobIDs
		for num := range 3 {
			err = PublishMessage(
				ctx,
				topic,
				TaskCode_B,
				usecase.TaskDataUsecaseA{Input: result + fmt.Sprintf("-%d", num)},
				nextJobIDs[num],
				cm.HistoryJobIDs,
				bjim,
				&cm.TraceID,
			)
			if err != nil {
				return errors.Wrap(err, "failed to publish message")
			}
		}

	case TaskCode_B:
		log.Println(cm.TaskCode)
		var taskDataUsecaseB usecase.TaskDataUsecaseB
		err := json.Unmarshal([]byte(cm.TaskData), &taskDataUsecaseB)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal message")
		}
		result := usecase.UsecaseB(taskDataUsecaseB)
		log.Println(result)
		msg.Ack()

		mux.Lock()
		defer mux.Unlock()
		err = PublishMessage(
			ctx,
			topic,
			TaskCode_C,
			usecase.TaskDataUsecaseB{Input: result},
			helper.GenerateID(),
			cm.HistoryJobIDs,
			cm.BarrierJobIDMap,
			&cm.TraceID,
		)
		if err != nil {
			return errors.Wrap(err, "failed to publish message")
		}

	case TaskCode_C:
		log.Println(cm.TaskCode)
		var taskDataUsecaseC usecase.TaskDataUsecaseC
		err := json.Unmarshal([]byte(cm.TaskData), &taskDataUsecaseC)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal message")
		}
		result := usecase.UsecaseC(taskDataUsecaseC)
		log.Println(result)
		log.Printf("[%s] ---------------------------All Task Process Finished %+v\n", cm.TraceID, cm.HistoryJobIDs)
		msg.Ack()
	default:
		return errors.New("unknown TaskCode")
	}
	return nil
}

func PublishMessage(
	ctx context.Context,
	topic *pubsub.Topic,
	nextTaskCode TaskCode,
	taskData any,
	jobID string,
	historyJobIDs []string,
	barrierJobIDMap map[TaskCode][]string,
	traceID *string,
) error {
	newHistoryJobIDs := append(historyJobIDs, jobID)

	taskDataBytes, err := json.Marshal(taskData)
	if err != nil {
		return errors.Wrap(err, "failed to marshal taskData")
	}

	tID := helper.GenerateID()
	if traceID != nil {
		tID = *traceID
	}
	data, err := json.Marshal(CustomMessage{
		JobID:           jobID,
		TaskCode:        nextTaskCode,
		TaskData:        taskDataBytes,
		BarrierJobIDMap: barrierJobIDMap,
		HistoryJobIDs:   newHistoryJobIDs,
		TraceID:         tID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to marshal message")
	}
	res := topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	id, err := res.Get(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to publish message")
	}
	log.Printf("[%s] Published a message; msg ID: %v\n", tID, id)
	return nil
}

func StartPublish(ctx context.Context, topic *pubsub.Topic) error {
	log.Println("Publishing message")
	for num := range 3 {
		err := PublishMessage(
			ctx,
			topic,
			TaskCode_A,
			usecase.TaskDataUsecaseA{Input: fmt.Sprintf("START-%d", num)},
			helper.GenerateID(),
			nil,
			nil,
			nil,
		)
		if err != nil {
			return errors.Wrap(err, "failed to publish message")
		}
		log.Printf("Published message %d\n", num)
	}
	return nil
}
