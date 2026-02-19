package cloud

import (
	"context"
	"errors"
	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	corev1 "k8s.io/api/core/v1"
)

type Identity struct {
	Name       string
	Identifier string // Identifier is email in gcp and arn in aws
}

type DesiredPermissions struct {
	Roles          []string          //Google
	InlinePolicies map[string]string //AWS
}

type AWS struct {
	IamClient *iam.Client
	OidcID    string
	MSK       MSK
}

type MSK struct {
	ARN       string
	ParsedARN arn.ARN
}

type TrustPolicyDocument struct {
	Version   string                 `json:"Version"`
	Statement []TrustPolicyStatement `json:"Statement"`
}

type TrustPolicyStatement struct {
	Effect    string                       `json:"Effect"`
	Principal map[string]string            `json:"Principal"`
	Action    string                       `json:"Action"`
	Condition map[string]map[string]string `json:"Condition,omitempty"`
}

type InlinePolicyDocument struct {
	Version   string                  `json:"Version"`
	Statement []InlinePolicyStatement `json:"Statement"`
}

type InlinePolicyStatement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type GCloud struct {
	ProjectID  string
	ClientRole string
}

type Dummy struct {
	GetServiceAccountErr    string `yaml:"getServiceAccountErr"`
	GetServiceAccountResult string `yaml:"getServiceAccountResult"`
	CreateServiceAccountErr string `yaml:"createServiceAccountErr"`
}

type CloudImpl interface {
	CreateIdentity(ctx context.Context, name string) (*Identity, error)
	GetIdentity(ctx context.Context, name string) (*Identity, error)
	DeleteIdentity(ctx context.Context, identity *Identity) error
	AddWorkloadIdentity(ctx context.Context, k8sNs, k8sSa, identityName string) error
	CheckWorkloadIdentity(ctx context.Context, k8sNs, k8sSa, identityName string) error
	GetPermissions(ctx context.Context, identity *Identity) (*DesiredPermissions, error)
	SetPermissions(ctx context.Context, identity *Identity, permissions *DesiredPermissions) error
	BuildDesiredPermissions(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, allowedPermissions []string) (*DesiredPermissions, error)
	EqualPermissions(ctx context.Context, want, have *DesiredPermissions) bool
	DeletePermissions(ctx context.Context, identity *Identity) error
	GetSAAnnotations(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) map[string]string
	IsSAReady(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, sa *corev1.ServiceAccount) bool
	CleanupSA(ctx context.Context, sa *corev1.ServiceAccount)
}

var ErrNotFound = errors.New("resource is not found")
