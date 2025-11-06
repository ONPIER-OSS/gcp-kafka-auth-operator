package kafkawrap

import (
	"context"
)

type KafkaImpl interface {
	CreateACL(ctx context.Context, username string, access []*TopicAccess) error
	DeleteACL(ctx context.Context, username string, access []*TopicAccess) error
	CreateTopic(ctx context.Context, name string, numPartition, replicationFactor int, config map[string]string) error
	RemoveTopic(ctx context.Context, name string) error
	ListTopics(ctx context.Context, hideInternal bool) ([]string, error)
	ListACLs(ctx context.Context, user string) ([]*TopicAccess, error)
}
