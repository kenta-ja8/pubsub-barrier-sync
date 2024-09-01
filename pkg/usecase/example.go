package usecase

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type TaskDataUsecaseA struct {
	Input string `json:"input"`
}

type TaskDataUsecaseB struct {
	Input string `json:"input"`
}

type TaskDataUsecaseC struct {
	Input string `json:"input"`
}

func UsecaseA(taskData TaskDataUsecaseA) string {
	log.Println("usecaseA", taskData)
	return taskData.Input + "/UsecaseA-Finished"
}

func UsecaseB(taskData TaskDataUsecaseB) string {
	log.Println("usecaseB", taskData)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	waitSec := 3 + r.Intn(5)
	waitTime := time.Duration(waitSec) * time.Second
	log.Println("----------------------------Waiting for", waitTime, taskData)
	time.Sleep(waitTime)

	log.Println("----------------------------Done waiting!", taskData)
	return fmt.Sprintf("%s/UsecaseB-%v-Finished", taskData.Input, waitSec)
}

func UsecaseC(taskData TaskDataUsecaseC) string {
	log.Println("usecaseC", taskData)
	return taskData.Input + "/UsecaseC-Finished"
}
