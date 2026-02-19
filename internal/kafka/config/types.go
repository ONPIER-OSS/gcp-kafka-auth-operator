package kafkaconfig

import (
	"context"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type AWSConfig struct {
	BootstrapServers string
	Region           string
}

type GCPConfig struct {
	BootstrapServers string
}

type KafkaConfigImpl interface {
	CreateAdmin(ctx context.Context) (*kafka.AdminClient, error)
}
