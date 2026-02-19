package kafkaconfig

import (
	"context"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

func NewAWSKafkaConfig(region, bootstrapServer string) KafkaConfigImpl {
	return &AWSConfig{
		BootstrapServers: bootstrapServer,
		Region:           region,
	}
}

func (a *AWSConfig) CreateAdmin(ctx context.Context) (*kafka.AdminClient, error) {
	log := logf.FromContext(ctx)
	log.Info("Creating aws kafka admin client")

	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":        a.BootstrapServers,
		"enable.auto.offset.store": false,
		"session.timeout.ms":       6000,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          "OAUTHBEARER",
	}

	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		log.Error(err, "Couldn't create new producer")
		return nil, err
	}

	go a.handleOAuthEvents(ctx, producer)
	admin, err := kafka.NewAdminClientFromProducer(producer)
	if err != nil {
		admin.Close()
		producer.Close()
		log.Error(err, "Couldn't create admin instance from producer")
		return nil, err
	}
	token, err := a.createToken(ctx)
	if err != nil {
		log.Error(err, "Couldn't create token")
		return nil, err
	}
	if err := producer.SetOAuthBearerToken(token); err != nil {
		producer.Close()
		log.Error(err, "Couldn't set oauth bearer token")
		return nil, err
	}
	return admin, nil
}

func (a *AWSConfig) createToken(ctx context.Context) (kafka.OAuthBearerToken, error) {
	log := logf.FromContext(ctx)

	token, exp, err := signer.GenerateAuthToken(context.TODO(), a.Region)
	if err != nil {
		log.Error(err, "Couldn't genrate auth token")
		return kafka.OAuthBearerToken{}, err
	}
	seconds := exp / 1000
	nanoseconds := (exp % 1000) * 1000000
	bearerToken := kafka.OAuthBearerToken{
		TokenValue: token,
		Expiration: time.Unix(seconds, nanoseconds),
	}
	return bearerToken, nil
}

func (a *AWSConfig) handleOAuthEvents(ctx context.Context, producer *kafka.Producer) {
	log := logf.FromContext(ctx)

	for event := range producer.Events() {
		switch event.(type) {
		case kafka.OAuthBearerTokenRefresh:
			token, err := a.createToken(ctx)
			if err != nil {
				log.Error(err, "Couldn't create token")
				return
			}
			if err := producer.SetOAuthBearerToken(token); err != nil {
				_ = producer.SetOAuthBearerTokenFailure(err.Error())
			}
		}
	}
}
