package helpers_test

import (
	"testing"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	"github.com/stretchr/testify/assert"
)

func TestStringToSlice(t *testing.T) {
	str := `test
test2
test3`
	assert.Equal(t, []string{"test", "test2", "test3"}, helpers.StringToSlice(str))
}

func TestGetHashFromKafkaUser(t *testing.T) {
	spec := &gcpkafkav1alpha1.KafkaUserSpec{
		ServiceAccountName: "someName",
		TopicAccess: []*gcpkafkav1alpha1.TopicAccess{{
			Topic: "t1",
			Role:  "readOnly",
		}},
		ExtraRoles: []string{
			"test",
		},
	}

	res, err := helpers.GetHashFromAnything(spec)
	assert.NoError(t, err)
	assert.Equal(t, "075953b08f99d85c149c3233bbfa22d56c8d3bd06b4fb4b549fd73187e7afdf2", res)
}

func TestGetHashFromExternalUser(t *testing.T) {
	username := "test"
	spec := &gcpkafkav1alpha1.ExternalKafkaUserSpec{
		Username: &username,
		TopicAccess: []*gcpkafkav1alpha1.TopicAccess{{
			Topic: "t1",
			Role:  "readOnly",
		}},
	}

	res, err := helpers.GetHashFromAnything(spec)
	assert.NoError(t, err)
	assert.Equal(t, "38f49398f89645233175dc5c83f5527cf9fa6428d5ccf9d07c4926d5e369cd0c", res)
}
