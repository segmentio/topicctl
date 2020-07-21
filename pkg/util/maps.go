package util

import (
	"hash/fnv"
	"math/rand"
	"sort"
)

type KeySorter func(map[int]int) []int

// SortedKeys returns the keys of the argument, sorted by value.
func SortedKeys(input map[int]int) []int {
	keys := []int{}

	for key := range input {
		keys = append(keys, key)
	}

	sort.Slice(
		keys, func(a, b int) bool {
			return keys[a] < keys[b]
		},
	)

	return keys
}

// ShuffledKeys returns a shuffled version of the keys in the
// argument map. The provided seedStr is hashed and used to seed
// the random number generator.
func ShuffledKeys(input map[int]int, seedStr string) []int {
	keys := SortedKeys(input)

	hash := fnv.New64()
	hash.Write([]byte(seedStr))

	random := rand.New(rand.NewSource(int64(hash.Sum64())))
	random.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	return keys
}

// SortedKeysByValue returns the keys in a map, sorted by the map values.
func SortedKeysByValue(input map[int]int, asc bool, keySorter KeySorter) []int {
	// First, sort the keys
	keys := keySorter(input)

	// Then, sort by value
	if asc {
		sort.Slice(
			keys, func(a, b int) bool {
				return input[keys[a]] < input[keys[b]]
			},
		)
	} else {
		sort.Slice(
			keys, func(a, b int) bool {
				return input[keys[a]] > input[keys[b]]
			},
		)
	}

	return keys
}
