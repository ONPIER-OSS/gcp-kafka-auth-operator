package kafkawrap

import (
	kafkaconfig "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka/config"
)

type KafkaConfluent struct {
	ConfigProvider kafkaconfig.KafkaConfigImpl
}

type KafkaDummy struct{}
