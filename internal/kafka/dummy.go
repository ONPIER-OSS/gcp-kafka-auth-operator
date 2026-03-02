package kafkawrap

import (
	"context"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func (k *KafkaDummy) CreateAdmin(ctx context.Context) (*kafka.AdminClient, error) {
	panic("unimplemented")
}

// ListACLs implements KafkaImpl.
func (k *KafkaDummy) ListACLs(ctx context.Context, admin *kafka.AdminClient, user string) ([]*TopicAccess, error) {
	panic("unimplemented")
}

// ListTopics implements KafkaImpl.
func (k *KafkaDummy) ListTopics(ctx context.Context, admin *kafka.AdminClient, _ bool) ([]string, error) {
	panic("unimplemented")
}

func NewKafkaDummy() KafkaImpl {
	return &KafkaDummy{}
}

// CreateACL implements KafkaImpl.
func (k *KafkaDummy) CreateACL(ctx context.Context, admin *kafka.AdminClient, username string, access []*TopicAccess) error {
	return nil
}

// CreateTopic implements KafkaImpl.
func (k *KafkaDummy) CreateTopic(ctx context.Context, admin *kafka.AdminClient, name string, numPartition int, replicationFactor int, config map[string]string) error {
	return nil
}

// DeleteACL implements KafkaImpl.
func (k *KafkaDummy) DeleteACL(ctx context.Context, admin *kafka.AdminClient, username string, access []*TopicAccess) error {
	return nil
}

// RemoveTopic implements KafkaImpl.
func (k *KafkaDummy) RemoveTopic(ctx context.Context, admin *kafka.AdminClient, name string) error {
	return nil
}
