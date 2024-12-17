package helper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateID(t *testing.T) {
	id := GenerateID()
	assert.NotEmpty(t, id, "Generated ID should not be empty")
}

func TestAllElementsInSlice(t *testing.T) {
	mainSlice := []string{"a", "b", "c", "d"}
	subSlice := []string{"a", "c"}

	assert.True(t, AllElementsInSlice(subSlice, mainSlice), "All elements of subSlice should be in mainSlice")

	subSlice = []string{"a", "e"}
	assert.False(t, AllElementsInSlice(subSlice, mainSlice), "Not all elements of subSlice are in mainSlice")
}

func TestRemoveElements(t *testing.T) {
	original := []string{"a", "b", "c", "d"}
	removeItems := []string{"b", "d"}

	expected := []string{"a", "c"}
	result := RemoveElements(original, removeItems)

	assert.Equal(t, expected, result, "Elements should be removed correctly")
}

func TestWaitGroupWithTimeout(t *testing.T) {
	var wg WaitGroupWithTimeout

	wg.Add(1)
	go func() {
		time.Sleep(2 * time.Second)
		wg.Done()
	}()

	assert.False(t, wg.WaitWithTimeout(1), "WaitGroup should timeout")
	assert.True(t, wg.WaitWithTimeout(3), "WaitGroup should complete within timeout")
}
