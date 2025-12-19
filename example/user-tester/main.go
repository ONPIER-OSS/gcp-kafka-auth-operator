package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-co-op/gocron/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Publish every 1 minutes
// Consume every 5 minutes

func main() {

	// Init metrics

	// Prepare the kafka connection
	projectID := os.Getenv("GCP_PROJECT")
	if len(projectID) == 0 {
		err := errors.New("project id is not set")
		log.Fatal(err)
	}
	region := os.Getenv("GCP_REGION")
	if len(region) == 0 {
		err := errors.New("region is not set")
		log.Fatal(err)
	}
	kafkaClusterName := os.Getenv("KAFKA_CLUSTER_NAME")
	if len(kafkaClusterName) == 0 {
		err := errors.New("kafka cluster name is not set")
		log.Fatal(err)
	}
	consumerGroupName := os.Getenv("KAFKA_CONSUMER_GROUP")
	if len(consumerGroupName) == 0 {
		err := errors.New("kafka consumer group is not set")
		log.Fatal(err)
	}

	bootstrapServer := fmt.Sprintf("bootstrap.%s.%s.managedkafka.%s.cloud.goog:9092", kafkaClusterName, region, projectID)

	config := &kafka.ConfigMap{
		"bootstrap.servers":                   bootstrapServer,
		"group.id":                            consumerGroupName,
		"enable.auto.offset.store":            false,
		"session.timeout.ms":                  20,
		"message.timeout.ms":                  20,
		"socket.timeout.ms":                   10,
		"security.protocol":                   "SASL_SSL",
		"sasl.mechanisms":                     "OAUTHBEARER",
		"sasl.oauthbearer.token.endpoint.url": "localhost:14293",
		"sasl.oauthbearer.client.id":          "unused",
		"sasl.oauthbearer.client.secret":      "unused",
		"sasl.oauthbearer.method":             "oidc",
	}

	kafkaTopicsRaw := os.Getenv("KAFKA_TOPICS")
	kafkaTopics := strings.Split(kafkaTopicsRaw, ",")

	counters := map[string]prometheus.Counter{}

	for _, topic := range kafkaTopics {
		counters[fmt.Sprintf("%s-publish-queued", topic)] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_event_publish_queued",
			Help: "The amount of successfully queued events",
			ConstLabels: prometheus.Labels{
				"topic":          topic,
				"consumer_group": consumerGroupName,
			},
		})
		counters[fmt.Sprintf("%s-publish-success", topic)] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_event_publish_success",
			Help: "The amount of successfully published events",
			ConstLabels: prometheus.Labels{
				"topic":          topic,
				"consumer_group": consumerGroupName,
			},
		})

		counters[fmt.Sprintf("%s-publish-failure", topic)] = promauto.NewCounter(prometheus.CounterOpts{
			Name: "kafka_event_publish_failure",
			Help: "The amount of failures while publishing events",
			ConstLabels: prometheus.Labels{
				"topic":          topic,
				"consumer_group": consumerGroupName,
			},
		})
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// create a scheduler
	s, err := gocron.NewScheduler()
	if err != nil {
		// handle error
	}
	// Listen to all the events on the default events channel

	// add a job to the scheduler
	for _, topic := range kafkaTopics {
		_, err = s.NewJob(
			gocron.DurationJob(
				10*time.Second,
			),
			gocron.NewTask(
				func() {
					deliveryChan := make(chan kafka.Event)
					go func() {
						for e := range deliveryChan {
							switch ev := e.(type) {
							case *kafka.Message:
								m := ev
								if m.TopicPartition.Error != nil {
									log.Error("Kafka error occurred: " + m.TopicPartition.Error.Error())
									counters[fmt.Sprintf("%s-publish-failure", topic)].Inc()
								} else {
									log.Info("Kafka event published successfully")
									counters[fmt.Sprintf("%s-publish-success", topic)].Inc()
								}

							default:
								log.Info("Ignored event")
							}
							// in this case the caller knows that this channel is used only
							// for one Produce call, so it can close it.
							close(deliveryChan)
						}
					}()

					val := fmt.Sprintf("Writing to kafka at %s", time.Now().String())

					err := producer.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          []byte(val),
					}, deliveryChan)

					if err != nil {
						close(deliveryChan)
						log.Error(err)
					} else {
						counters[fmt.Sprintf("%s-publish-queued", topic)].Inc()
					}
				},
			),
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	defer s.Shutdown()
	// start the scheduler
	s.Start()
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2112", nil)
}
