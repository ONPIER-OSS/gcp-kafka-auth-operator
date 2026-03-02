package cloud

import (
	"context"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

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
