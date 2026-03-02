package kafkawrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	kafkaconfig "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka/config"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func NewKafkaConfluent(ctx context.Context, env, region, bootstrapServer string) (KafkaImpl, error) {

	kafkaInstance := &KafkaConfluent{}

	configProvider, err := kafkaconfig.NewKafkaConfigInstance(env, region, bootstrapServer)
	if err != nil {
		return nil, err
	}

	kafkaInstance.ConfigProvider = configProvider
	return kafkaInstance, nil
}

func (kc *KafkaConfluent) CreateAdmin(ctx context.Context) (*kafka.AdminClient, error) {
	return kc.ConfigProvider.CreateAdmin(ctx)
}

// CreateTopic implements KafkaImpl.
func (kc *KafkaConfluent) CreateTopic(ctx context.Context, admin *kafka.AdminClient, name string, numPartitions, replicationFactor int, config map[string]string) error {
	log := log.FromContext(ctx)

	topic := kafka.TopicSpecification{
		Topic:             name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config:            config,
	}
	topics := []kafka.TopicSpecification{topic}
	res, err := admin.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(time.Minute*2))
	if err != nil {
		log.Error(err, "Couldn't remove ACLs")
		return err
	}
	log.Info("Topic is created", "result", fmt.Sprintf("%v", res))
	return nil
}

// RemoveTopic implements KafkaImpl.
func (kc *KafkaConfluent) RemoveTopic(ctx context.Context, admin *kafka.AdminClient, name string) error {
	log := log.FromContext(ctx)

	res, err := admin.DeleteTopics(ctx, []string{name}, kafka.SetAdminOperationTimeout(time.Minute*2))
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

func (kc *KafkaConfluent) DeleteACL(ctx context.Context, admin *kafka.AdminClient, user string, access []*TopicAccess) (err error) {
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
	_, err = admin.DeleteACLs(ctx, bindingFilters)
	if err != nil {
		log.Error(err, "Couldn't remove ACLs")
		return err
	}
	return nil
}

func (kc *KafkaConfluent) CreateACL(ctx context.Context, admin *kafka.AdminClient, user string, access []*TopicAccess) (err error) {
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
	_, err = admin.CreateACLs(ctx, bindings)
	if err != nil {
		log.Error(err, "Couldn't create ACLs")
		return err
	}

	return nil
}

// ListTopics implements KafkaImpl.
func (kc *KafkaConfluent) ListTopics(ctx context.Context, admin *kafka.AdminClient, hideInternal bool) ([]string, error) {
	log := log.FromContext(ctx)

	metadata, err := admin.GetMetadata(nil, true, 30)
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

func (kc *KafkaConfluent) ListACLs(ctx context.Context, admin *kafka.AdminClient, user string) ([]*TopicAccess, error) {
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
	res, err := admin.DescribeACLs(ctx, filter)
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
