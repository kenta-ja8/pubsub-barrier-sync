package usecase

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUsecaseA(t *testing.T) {
	taskData := TaskDataUsecaseA{Input: "test"}
	result := UsecaseA(taskData)
	expected := "test/UsecaseA-Finished"
	assert.Equal(t, expected, result)
}

func TestUsecaseB(t *testing.T) {
	taskData := TaskDataUsecaseB{Input: "test"}
	result := UsecaseB(taskData)
	expected := "test/UsecaseB-3-Finished" // This is a bit tricky due to the random delay
	assert.Contains(t, result, "test/UsecaseB-")
	assert.Contains(t, result, "-Finished")
}

func TestUsecaseC(t *testing.T) {
	taskData := TaskDataUsecaseC{Input: "test"}
	result := UsecaseC(taskData)
	expected := "test/UsecaseC-Finished"
	assert.Equal(t, expected, result)
}

func TestUsecaseGeneric(t *testing.T) {
	taskData := TaskDataGeneric{Input: "test"}
	result := UsecaseGeneric(taskData)
	expected := "test/UsecaseGeneric-Finished"
	assert.Equal(t, expected, result)
}

func TestUsecaseWithDelay(t *testing.T) {
	taskData := TaskDataGeneric{Input: "test"}
	result := UsecaseWithDelay(taskData, 5)
	expected := "test/UsecaseWithDelay-5-Finished"
	assert.Equal(t, expected, result)
}
