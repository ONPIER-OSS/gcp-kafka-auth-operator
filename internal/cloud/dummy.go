package cloud

import (
	"context"
	"errors"
	"os"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// AddWorkloadIdentityBinding implements CloudImpl.
func (d *Dummy) AddWorkloadIdentity(ctx context.Context, k8sNs, k8sSa string, cloudSa string) error {
	panic("unimplemented")
}

// CheckWorkloadIdentityBinding implements CloudImpl.
func (d *Dummy) CheckWorkloadIdentity(ctx context.Context, k8sNs, k8sSa string, cloudSa string) error {
	panic("unimplemented")
}

// GetIAMBindings implements CloudImpl.
func (d *Dummy) GetPermissions(ctx context.Context, identity *Identity) (*DesiredPermissions, error) {
	panic("unimplemented")
}

// SetIAMBindings implements CloudImpl.
func (d *Dummy) SetPermissions(ctx context.Context, identity *Identity, permissions *DesiredPermissions) error {
	panic("unimplemented")
}

func (d *Dummy) BuildDesiredPermissions(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, allowedPermissions []string) (*DesiredPermissions, error) {
	panic("unimplemented")
}

func (d *Dummy) DeletePermissions(ctx context.Context, identity *Identity) error {
	panic("unimplemented")
}

func (d *Dummy) EqualPermissions(ctx context.Context, want, have *DesiredPermissions) bool {
	panic("unimplemented")
}

// GetServiceAccount implements CloudImpl.
func (d *Dummy) GetIdentity(ctx context.Context, name string) (*Identity, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Getting a service account")
	if len(d.GetServiceAccountErr) > 0 {
		err := errors.New(d.GetServiceAccountErr)
		log.Error(err, "Couldn't get a service account")
		return nil, err
	}
	if len(d.GetServiceAccountResult) > 0 {
		return &Identity{
			Name:       d.GetServiceAccountResult,
			Identifier: d.GetServiceAccountResult,
		}, nil
	}
	return nil, nil
}

func (d *Dummy) DeleteIdentity(ctx context.Context, identity *Identity) error {
	panic("unimplemented")
}

// CreateServiceAccount implements CloudImpl.
func (d *Dummy) CreateIdentity(ctx context.Context, name string) (*Identity, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Creating a service account")
	if len(d.CreateServiceAccountErr) > 0 {
		err := errors.New(d.CreateServiceAccountErr)
		log.Error(err, "Couldn't create a service account")
		return nil, err
	}
	return &Identity{
		Name:       name,
		Identifier: "dummy",
	}, nil
}

func NewDummyInstance(configPath string) (CloudImpl, error) {
	cloud := &Dummy{}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cloud); err != nil {
		return nil, err
	}
	return cloud, nil
}

func (d *Dummy) GetSAAnnotations(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) map[string]string {
	panic("unimplemented")
}

func (d *Dummy) IsSAReady(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, sa *corev1.ServiceAccount) bool {
	panic("unimplemented")
}

func (d *Dummy) CleanupSA(ctx context.Context, sa *corev1.ServiceAccount) {
	panic("unimplemented")
}
