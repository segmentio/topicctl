package util

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/segmentio/kafka-go"
)

// KeySorter is a type for a function that sorts integer keys based on their values in a map.
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

// merges 2 maps into a single map object
func MergeMaps(a map[string]interface{}, b map[string]interface{}) map[string]interface{} {
	for k, v := range b {
		a[k] = v
	}
	return a
}

// prints map of changes being made to stdout
func PrintChangesMap(changesMap map[string]interface{}) error {
	jsonChanges, err := json.Marshal(changesMap)
	if err != nil {
		return err
	}
	fmt.Printf("Map of changes: %s\n", jsonChanges)
	return nil
}

// processes TopicConfig into changes map
func ProcessTopicConfigIntoMap(topicName string, topicConfig kafka.TopicConfig) (map[string]interface{}, error) {
	changes := make(map[string]interface{})
	// add newly created topic to changes json object
	changes[topicName] = topicConfig
	// encode and decode changes as json to convert value from TopicConfig to map
	// TODO: better way of doing this?
	changesJson, err := json.Marshal(changes)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(changesJson, &changes); err != nil {
		return nil, err
	}
	changes[topicName].(map[string]interface{})["Action"] = "create"
	return changes, nil
}
