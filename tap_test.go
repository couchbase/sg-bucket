package sgbucket

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDCPEncodeXattrs(t *testing.T) {
	body := []byte(`{"name":"the document body"}`)
	xattrs := []Xattr{
		{Name: "_sync", Value: []byte(`{"rev":1234}`)},
		{Name: "swim", Value: []byte(`{"stroke":"dolphin"}`)},
		{Name: "empty", Value: []byte(``)},
	}

	value := EncodeValueWithXattrs(body, xattrs...)
	gotBody, gotXattrs, err := DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, body, gotBody)
		assert.Equal(t, xattrs, gotXattrs)
	}

	// Try an empty body:
	emptyBody := []byte{}
	value = EncodeValueWithXattrs(emptyBody, xattrs...)
	gotBody, gotXattrs, err = DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, emptyBody, gotBody)
		assert.Equal(t, xattrs, gotXattrs)
	}

	// Try zero xattrs:
	value = EncodeValueWithXattrs(body)
	gotBody, gotXattrs, err = DecodeValueWithXattrs(value)
	if assert.NoError(t, err) {
		assert.Equal(t, body, gotBody)
		assert.Equal(t, 0, len(gotXattrs))
	}
}
