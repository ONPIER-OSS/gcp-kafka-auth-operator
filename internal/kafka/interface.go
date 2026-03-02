package kafkawrap

import (
	"context"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaImpl interface {
	CreateAdmin(ctx context.Context) (*kafka.AdminClient, error)
	CreateACL(ctx context.Context, admin *kafka.AdminClient, username string, access []*TopicAccess) error
	DeleteACL(ctx context.Context, admin *kafka.AdminClient, username string, access []*TopicAccess) error
	CreateTopic(ctx context.Context, admin *kafka.AdminClient, name string, numPartition, replicationFactor int, config map[string]string) error
	RemoveTopic(ctx context.Context, admin *kafka.AdminClient, name string) error
	ListTopics(ctx context.Context, admin *kafka.AdminClient, hideInternal bool) ([]string, error)
	ListACLs(ctx context.Context, admin *kafka.AdminClient, user string) ([]*TopicAccess, error)
}
