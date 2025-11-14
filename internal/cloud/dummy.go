package cloud

import (
	"context"
	"errors"
	"os"

	"gopkg.in/yaml.v3"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type Dummy struct {
	GetServiceAccountErr    string `yaml:"getServiceAccountErr"`
	GetServiceAccountResult string `yaml:"getServiceAccountResult"`
	CreateServiceAccountErr string `yaml:"createServiceAccountErr"`
}

// AddWorkloadIdentityBinding implements CloudImpl.
func (d *Dummy) AddWorkloadIdentityBinding(ctx context.Context, k8sSa string, cloudSa string) error {
	panic("unimplemented")
}

// CheckWorkloadIdentityBinding implements CloudImpl.
func (d *Dummy) CheckWorkloadIdentityBinding(ctx context.Context, k8sSa string, cloudSa string) error {
	panic("unimplemented")
}

// GetIAMBindings implements CloudImpl.
func (d *Dummy) GetIAMBindings(ctx context.Context, cloudSaID string) ([]string, error) {
	panic("unimplemented")
}

// SetIAMBindings implements CloudImpl.
func (d *Dummy) SetIAMBindings(ctx context.Context, cloudSaID string, roles []string) error {
	panic("unimplemented")
}

// GetServiceAccount implements CloudImpl.
func (d *Dummy) GetServiceAccount(ctx context.Context, name string) (*ServiceAccount, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Getting a service account")
	if len(d.GetServiceAccountErr) > 0 {
		err := errors.New(d.GetServiceAccountErr)
		log.Error(err, "Couldn't get a service account")
		return nil, err
	}
	if len(d.GetServiceAccountResult) > 0 {
		return &ServiceAccount{
			Name:       d.GetServiceAccountResult,
			Identifier: d.GetServiceAccountResult,
		}, nil
	}
	return nil, nil
}

// CreateServiceAccount implements CloudImpl.
func (d *Dummy) CreateServiceAccount(ctx context.Context, name string) (*ServiceAccount, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Creating a service account")
	if len(d.CreateServiceAccountErr) > 0 {
		err := errors.New(d.CreateServiceAccountErr)
		log.Error(err, "Couldn't create a service account")
		return nil, err
	}
	return &ServiceAccount{
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
