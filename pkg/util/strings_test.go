package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateStringSuffix(t *testing.T) {
	resultLong, omittedLong := TruncateStringSuffix("01234567890123456789", 10)
	assert.Equal(t, "0123456...", resultLong)
	assert.Equal(t, 13, omittedLong)

	resultShort, omittedShort := TruncateStringSuffix("012345", 10)
	assert.Equal(t, "012345", resultShort)
	assert.Equal(t, 0, omittedShort)
}

func TestTruncateStringMiddle(t *testing.T) {
	resultLong, omittedLong := TruncateStringMiddle("01234567890123456789", 10, 3)
	assert.Equal(t, "0123...789", resultLong)
	assert.Equal(t, 13, omittedLong)

	resultShort, omittedShort := TruncateStringMiddle("012345", 10, 3)
	assert.Equal(t, "012345", resultShort)
	assert.Equal(t, 0, omittedShort)
}
