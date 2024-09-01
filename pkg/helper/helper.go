package helper

import (
	"slices"

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
