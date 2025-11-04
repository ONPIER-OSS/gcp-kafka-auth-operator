package helpers_test

import (
	"testing"

	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	"github.com/stretchr/testify/assert"
)

func TestStringToSlice(t *testing.T) {
	str := `test
test2
test3`
	assert.Equal(t, []string{"test", "test2", "test3"}, helpers.StringToSlice(str))
}
