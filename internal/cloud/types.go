package cloud

import (
	"errors"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

type Identity struct {
	Name       string
	Identifier string // Identifier is email in gcp and arn in aws
}

type DesiredPermissions struct {
	Roles          []string          // Google
	InlinePolicies map[string]string // AWS
}

type AWS struct {
	Config aws.Config
	OidcID string
	MSK    MSK
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

var ErrNotFound = errors.New("resource is not found")
