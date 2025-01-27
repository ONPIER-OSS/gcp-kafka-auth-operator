package main

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	projectID := os.Getenv("GCP_PROJECT")
	region := os.Getenv("GCP_REGION")
	kafkaClusterName := os.Getenv("KAFKA_CLUSTER_NAME")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	bootstrapServer := fmt.Sprintf("bootstrap.%s.%s.managedkafka.%s.cloud.goog:9092", kafkaClusterName, region, projectID)
	consumerGroupName := fmt.Sprintf("e2e-test-%s-consumer", kafkaTopic)

	config := &kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServer,
		"group.id":                            consumerGroupName,
		"enable.auto.offset.store":            false,
		"session.timeout.ms":                  6000,
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanisms":                     "OAUTHBEARER",
		"sasl.oauthbearer.token.endpoint.url": "localhost:14293",
		"sasl.oauthbearer.client.id":          "unused",
		"sasl.oauthbearer.client.secret":      "unused",
		"sasl.oauthbearer.method":             "oidc",
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Assign partitions and seek to the earliest offset
	partitions := []kafka.TopicPartition{
		{Topic: &kafkaTopic, Partition: 0, Offset: kafka.OffsetBeginning},
	}

	err = consumer.Assign(partitions)
	if err != nil {
		log.Fatalf("Failed to assign partitions: %v", err)
	}

	log.Println("Waiting for the first message...")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v\n", err)
		} else {
			log.Printf("Consumed message: %s\n", string(msg.Value))
		}
	}
}
