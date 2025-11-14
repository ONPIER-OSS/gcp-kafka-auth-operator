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

func TestGetHashFromKafkaUserEqual(t *testing.T) {
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

	specCopy := spec.DeepCopy()

	resOriginal, err := helpers.GetHashFromAnything(spec)
	assert.NoError(t, err)

	resCopy, err := helpers.GetHashFromAnything(specCopy)
	assert.NoError(t, err)
	assert.Equal(t, resOriginal, resCopy)
}

func TestGetHashFromExternalKafkaUserNotEqual(t *testing.T) {
	usernameOne := "testone"
	usernameTwo := "testtwo"
	specOne := &gcpkafkav1alpha1.ExternalKafkaUserSpec{
		Username: &usernameOne,
		TopicAccess: []*gcpkafkav1alpha1.TopicAccess{{
			Topic: "t1",
			Role:  "readOnly",
		}},
	}

	specTwo := &gcpkafkav1alpha1.ExternalKafkaUserSpec{
		Username: &usernameTwo,
		TopicAccess: []*gcpkafkav1alpha1.TopicAccess{{
			Topic: "t2",
			Role:  "readWrite",
		}},
	}

	resOne, err := helpers.GetHashFromAnything(specOne)
	assert.NoError(t, err)

	resTwo, err := helpers.GetHashFromAnything(specTwo)
	assert.NoError(t, err)
	assert.NotEqual(t, resOne, resTwo)
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

	specCopy := spec.DeepCopy()
	resOriginal, err := helpers.GetHashFromAnything(spec)
	assert.NoError(t, err)

	resCopy, err := helpers.GetHashFromAnything(specCopy)
	assert.NoError(t, err)
	assert.Equal(t, resCopy, resOriginal)
}

func TestStringSanitizeShort(t *testing.T) {
	str := "test**test__test-test"
	expect := "test--test--test-test"
	assert.Equal(t, expect, helpers.StringSanitize(str, 40))
}

func TestStringSanitizeLong(t *testing.T) {
	str := "test**test__test-test"
	expect := "t-38a46a1f"
	assert.Equal(t, expect, helpers.StringSanitize(str, 10))
}
