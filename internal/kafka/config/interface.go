package kafkaconfig

import (
	"context"

	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaConfigImpl interface {
	CreateAdmin(ctx context.Context) (*kafka.AdminClient, error)
}
