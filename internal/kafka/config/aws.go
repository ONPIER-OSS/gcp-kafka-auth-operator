package kafkaconfig

import (
	"context"
	"time"

	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
		"bootstrap.servers": a.BootstrapServers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "OAUTHBEARER",
	}

	admin, err := kafka.NewAdminClient(kafkaConfig)
	if err != nil {
		log.Error(err, "Couldn't create admin instance from producer")
		return nil, err
	}

	token, err := a.createToken(ctx)
	if err != nil {
		log.Error(err, "Couldn't create token")
		return nil, err
	}
	if err := admin.SetOAuthBearerToken(token); err != nil {
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
