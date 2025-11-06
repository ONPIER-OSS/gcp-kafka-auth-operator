package cloud

import (
	"context"
	"errors"
)

type CloudImpl interface {
	CreateServiceAccount(ctx context.Context, name string) (*ServiceAccount, error)
	GetServiceAccount(ctx context.Context, name string) (*ServiceAccount, error)
	AddWorkloadIdentityBinding(ctx context.Context, k8sSa, cloudSa string) error
	CheckWorkloadIdentityBinding(ctx context.Context, k8sSa, cloudSa string) error
	GetIAMBindings(ctx context.Context, cloudSaID string) ([]string, error)
	SetIAMBindings(ctx context.Context, cloudSaID string, roles []string) error
}

type ServiceAccount struct {
	Name string
	// Identifier is email in gcp and arn in aws
	Identifier string
}

var ErrNotFound = errors.New("resource is not found")
