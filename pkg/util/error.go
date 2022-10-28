package util

import (
	"fmt"
)

func ErrorsHasError(errors map[string]error) error {
	var hasErrors bool
	for _, err := range errors {
		if err != nil {
			hasErrors = true
			break
		}
	}
	if hasErrors {
		return fmt.Errorf("errors when creating topics: %+v", errors)
	}
	return nil
}
