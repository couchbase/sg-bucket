package sgbucket

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidDataStoreName(t *testing.T) {

	validDataStoreNames := [][2]string{
		{"myScope", "myCollection"},
		{"ABCabc123_-%", "ABCabc123_-%"},
		{"_default", "myCollection"},
		{"_default", "_default"},
	}

	invalidDataStoreNames := [][2]string{
		{"a:1", "a:1"},
		{"_a", "b"},
		{"a", "_b"},
		{"%a", "b"},
		{"%a", "b"},
		{"a", "%b"},
		{"myScope", "_default"},
		{"_default", "a:1"},
	}

	for _, validPair := range validDataStoreNames {
		assert.True(t, IsValidDataStoreName(validPair[0], validPair[1]),
			"(%q, %q) should be valid", validPair[0], validPair[1])
	}
	for _, invalidPair := range invalidDataStoreNames {
		assert.False(t, IsValidDataStoreName(invalidPair[0], invalidPair[1]),
			"(%q, %q) should be invalid", invalidPair[0], invalidPair[1])
	}
}
