package helper

import (
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

func GenerateID() string {
	return uuid.New().String()
}

func AllElementsInSlice(subSlice, mainSlice []string) bool {
	for _, item := range subSlice {
		if !slices.Contains(mainSlice, item) {
			return false
		}
	}
	return true
}

func RemoveElements(original, removeItems []string) []string {
	result := make([]string, 0, len(original))
	for _, item := range original {
		if !slices.Contains(removeItems, item) {
			result = append(result, item)
		}
	}
	return result
}

// New generic helper functions for barrier synchronization

// WaitGroupWithTimeout is a wrapper around sync.WaitGroup that adds a timeout feature.
type WaitGroupWithTimeout struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (wg *WaitGroupWithTimeout) Add(delta int) {
	wg.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one.
func (wg *WaitGroupWithTimeout) Done() {
	wg.wg.Done()
}

// WaitWithTimeout waits for the WaitGroup counter to reach zero or the timeout to expire.
func (wg *WaitGroupWithTimeout) WaitWithTimeout(timeout int) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.wg.Wait()
	}()
	select {
	case <-c:
		return true // completed normally
	case <-time.After(time.Duration(timeout) * time.Second):
		return false // timed out
	}
}
