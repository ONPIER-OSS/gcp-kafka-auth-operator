package kafkawrap

import "context"

type KafkaDummy struct{}

// ListACLs implements KafkaImpl.
func (k *KafkaDummy) ListACLs(ctx context.Context, user string) ([]*TopicAccess, error) {
	panic("unimplemented")
}

// ListTopics implements KafkaImpl.
func (k *KafkaDummy) ListTopics(ctx context.Context, _ bool) ([]string, error) {
	panic("unimplemented")
}

func NewKafkaDummy() KafkaImpl {
	return &KafkaDummy{}
}

// CreateACL implements KafkaImpl.
func (k *KafkaDummy) CreateACL(ctx context.Context, username string, access []*TopicAccess) error {
	return nil
}

// CreateTopic implements KafkaImpl.
func (k *KafkaDummy) CreateTopic(ctx context.Context, name string, numPartition int, replicationFactor int, config map[string]string) error {
	return nil
}

// DeleteACL implements KafkaImpl.
func (k *KafkaDummy) DeleteACL(ctx context.Context, username string, access []*TopicAccess) error {
	return nil
}

// RemoveTopic implements KafkaImpl.
func (k *KafkaDummy) RemoveTopic(ctx context.Context, name string) error {
	return nil
}
