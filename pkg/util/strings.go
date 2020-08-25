package util

import "fmt"

// TruncateStringSuffix truncates a string by replacing the trailing characters with
// "..." if needed.
func TruncateStringSuffix(input string, maxLen int) (string, int) {
	if len(input)-3 <= maxLen {
		return input, 0
	}

	numOmitted := len(input) - (maxLen - 3)
	return fmt.Sprintf("%s...", input[:maxLen-3]), numOmitted
}

// TruncateStringMiddle truncates a string by replacing characters in the middle with
// "..." if needed.
func TruncateStringMiddle(input string, maxLen int, suffixLen int) (string, int) {
	if len(input)-3 <= maxLen {
		return input, 0
	}

	suffix := input[len(input)-suffixLen:]
	prefix := input[:maxLen-suffixLen-3]

	numOmitted := len(input) - len(prefix) - len(suffix)
	return fmt.Sprintf("%s...%s", prefix, suffix), numOmitted
}
