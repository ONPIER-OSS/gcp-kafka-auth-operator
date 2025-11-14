package kafkawrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type KafkaConfluent struct {
	AdminClient *kafka.AdminClient
}

func NewKafkaConfluent(projectID, kafkaClusterName, region string) (KafkaImpl, error) {
	kafkaInstance := &KafkaConfluent{}
	var err error
	timeout := 30 * time.Second
	_, err = net.DialTimeout("tcp", "localhost:14293", timeout)
	if err != nil {
		return nil, errors.New("proxy isn't reachable")
	}
	bootstrapServer := fmt.Sprintf("bootstrap.%s.%s.managedkafka.%s.cloud.goog:9092",
		kafkaClusterName, region, projectID)
	config := &kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServer,
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
	kafkaInstance.AdminClient, err = kafka.NewAdminClient(config)
	if err != nil {
		return nil, err
	}

	return kafkaInstance, nil
}

// CreateTopic implements KafkaImpl.
func (kc *KafkaConfluent) CreateTopic(ctx context.Context, name string, numPartitions, replicationFactor int, config map[string]string) error {
	log := log.FromContext(ctx)
	topic := kafka.TopicSpecification{
		Topic:             name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config:            config,
	}
	topics := []kafka.TopicSpecification{topic}
	res, err := kc.AdminClient.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(time.Minute*2))
	if err != nil {
		log.Error(err, "Couldn't remove ACLs")
		return err
	}
	log.Info("Topic is created", "result", fmt.Sprintf("%v", res))
	return nil
}

// RemoveTopic implements KafkaImpl.
func (kc *KafkaConfluent) RemoveTopic(ctx context.Context, name string) error {
	log := log.FromContext(ctx)
	res, err := kc.AdminClient.DeleteTopics(ctx, []string{name}, kafka.SetAdminOperationTimeout(time.Minute*2))
	if err != nil {
		log.Error(err, "Couldn't remove ACLs")
		return err
	}
	log.Info("Topic is removed", "result", fmt.Sprintf("%v", res))
	return nil
}

type TopicAccess struct {
	Topic     string
	Operation kafka.ACLOperation
}

func NewTopicAccess(topic, access string) (*TopicAccess, error) {
	switch access {
	case consts.ACCESS_READ_WRITE:
		return &TopicAccess{
			Topic:     topic,
			Operation: kafka.ACLOperationAll,
		}, nil
	case consts.ACCESS_READ_ONLY:
		return &TopicAccess{
			Topic:     topic,
			Operation: kafka.ACLOperationRead,
		}, nil
	default:
		return nil, errors.New("unknown access")
	}
}

func ParseTopicAccess(access *TopicAccess) (topic, role string, err error) {
	switch access.Operation {
	case kafka.ACLOperationAll:
		return access.Topic, consts.ACCESS_READ_WRITE, nil
	case kafka.ACLOperationRead:
		return access.Topic, consts.ACCESS_READ_ONLY, nil
	default:
		return "", "", errors.New("unknown access")
	}
}

func (kc *KafkaConfluent) DeleteACL(ctx context.Context, user string, access []*TopicAccess) (err error) {
	log := log.FromContext(ctx)

	principal := fmt.Sprintf("User:%s", user)
	bindingFilters := kafka.ACLBindingFilters{}
	for _, a := range access {
		aclsToDelete := kafka.ACLBindingFilter{
			Type:                kafka.ResourceTopic,
			Name:                a.Topic,
			ResourcePatternType: kafka.ResourcePatternTypeLiteral,
			Principal:           principal,
			Host:                "*",
			Operation:           a.Operation,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		}
		bindingFilters = append(bindingFilters, aclsToDelete)
	}
	_, err = kc.AdminClient.DeleteACLs(ctx, bindingFilters)
	if err != nil {
		log.Error(err, "Couldn't remove ACLs")
		return err
	}
	return nil
}

func (kc *KafkaConfluent) CreateACL(ctx context.Context, user string, access []*TopicAccess) (err error) {
	log := log.FromContext(ctx)

	principal := fmt.Sprintf("User:%s", user)
	bindings := kafka.ACLBindings{}
	for _, a := range access {
		binding := kafka.ACLBinding{
			Type:                kafka.ResourceTopic,
			Name:                a.Topic,
			Principal:           principal,
			PermissionType:      kafka.ACLPermissionTypeAllow,
			Operation:           a.Operation,
			Host:                "*",
			ResourcePatternType: kafka.ResourcePatternTypeLiteral,
		}
		bindings = append(bindings, binding)
	}
	_, err = kc.AdminClient.CreateACLs(ctx, bindings)
	if err != nil {
		log.Error(err, "Couldn't create ACLs")
		return err
	}

	return nil
}

// ListTopics implements KafkaImpl.
func (kc *KafkaConfluent) ListTopics(ctx context.Context, hideInternal bool) ([]string, error) {
	log := log.FromContext(ctx)
	metadata, err := kc.AdminClient.GetMetadata(nil, true, 30)
	if err != nil {
		log.Error(err, "Couldn't get metadata")
		return nil, err
	}
	out := []string{}
	for _, topic := range metadata.Topics {
		if hideInternal {
			if strings.HasPrefix(topic.Topic, "__") {
				continue
			}
		}
		out = append(out, topic.Topic)
	}

	return out, nil
}

func (kc *KafkaConfluent) ListACLs(ctx context.Context, user string) ([]*TopicAccess, error) {
	log := log.FromContext(ctx)

	principal := fmt.Sprintf("User:%s", user)
	filter := kafka.ACLBindingFilter{
		Type:                kafka.ResourceTopic,
		Principal:           principal,
		ResourcePatternType: kafka.ResourcePatternTypeAny,
		Operation:           kafka.ACLOperationAny,
		PermissionType:      kafka.ACLPermissionTypeAllow,
		Host:                "*",
	}
	res, err := kc.AdminClient.DescribeACLs(ctx, filter)
	if err != nil {
		log.Error(err, "Couldn't list ACLs")
		return nil, err
	}

	accessList := []*TopicAccess{}
	for _, bind := range res.ACLBindings {
		log.Info("Result of descrbe", "bind", bind.Name, "operation", bind.Operation)
		access := &TopicAccess{
			Topic:     bind.Name,
			Operation: bind.Operation,
		}
		accessList = append(accessList, access)
	}

	return accessList, nil
}
