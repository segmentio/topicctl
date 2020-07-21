package util

import (
	"reflect"
)

// CopyInts copies a slice of ints.
func CopyInts(input []int) []int {
	results := make([]int, len(input))
	copy(results, input)
	return results
}

// SameElements determines whether two int slices have the
// same elements (in any order).
func SameElements(slice1 []int, slice2 []int) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	slice1Counts := map[int]int{}
	for _, s := range slice1 {
		slice1Counts[s]++
	}

	slice2Counts := map[int]int{}
	for _, s := range slice2 {
		slice2Counts[s]++
	}

	return reflect.DeepEqual(slice1Counts, slice2Counts)
}
