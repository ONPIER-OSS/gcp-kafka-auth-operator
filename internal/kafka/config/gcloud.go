package kafkaconfig

import (
	"context"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"net"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

func NewGCPKafkaConfig(bootstrapServer string) KafkaConfigImpl {
	return &GCPConfig{
		BootstrapServers: bootstrapServer,
	}
}

func (g *GCPConfig) CreateAdmin(ctx context.Context) (*kafka.AdminClient, error) {
	log := logf.FromContext(ctx)
	log.Info("Creating gcloud kafka admin client")

	var err error
	timeout := 30 * time.Second
	_, err = net.DialTimeout("tcp", "localhost:14293", timeout)
	if err != nil {
		panic(err)
	}

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":        g.BootstrapServers,
		"enable.auto.offset.store": false,
		"session.timeout.ms":       6000,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "OAUTHBEARER",
		// The auth proxy must be running in the port 14293
		"sasl.oauthbearer.token.endpoint.url": "localhost:14293",
		"sasl.oauthbearer.client.id":          "unused",
		"sasl.oauthbearer.client.secret":      "unused",
		"sasl.oauthbearer.method":             "oidc",
	}
	admin, err := kafka.NewAdminClient(kafkaConfig)
	if err != nil {
		log.Error(err, "Couldn't create admin client")
		return nil, err
	}
	return admin, nil
}
