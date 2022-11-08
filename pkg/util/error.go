package util

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

func KafkaErrorsToErr(errors map[string]error) error {
	var hasErrors bool
	for _, err := range errors {
		if err != nil {
			hasErrors = true
			break
		}
	}
	if hasErrors {
		return fmt.Errorf("%+v", errors)
	}
	return nil
}

func IncrementalAlterConfigsResponseResourcesError(resources []kafka.IncrementalAlterConfigsResponseResource) error {
	errors := map[string]error{}
	var hasErrors bool
	for _, resource := range resources {
		if resource.Error != nil {
			hasErrors = true
			errors[resource.ResourceName] = resource.Error
		}
	}
	if hasErrors {
		return fmt.Errorf("%+v", errors)
	}
	return nil
}
