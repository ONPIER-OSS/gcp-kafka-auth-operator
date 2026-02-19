package kafkaconfig

import (
	"fmt"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
)

func NewKafkaConfigInstance(env, region, bootstrapServer string) (KafkaConfigImpl, error) {
	switch env {
	case consts.EnvGCP:
		return NewGCPKafkaConfig(bootstrapServer), nil
	case consts.EnvAWS:
		return NewAWSKafkaConfig(region, bootstrapServer), nil
	default:
		return nil, fmt.Errorf("unsupported env: %s", env)
	}
}
