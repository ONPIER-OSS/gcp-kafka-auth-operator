package cloud

import (
	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"context"
	"errors"
	"fmt"
	"reflect"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"slices"
	"testing"
)

// GetIAMBindings implements CloudImpl.
// TODO: It should be possible to use the googleapi lib
func (g *GCloud) GetPermissions(ctx context.Context, identity *Identity) (*DesiredPermissions, error) {
	log := logf.FromContext(ctx)
	log.Info("Getting IAM bindings")

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, err
	}
	defer func(client *resourcemanager.ProjectsClient) {
		if err := client.Close(); err != nil {
			log.Error(err, "Couldn't close the google client")
		}
	}(client)

	// Get the current IAM policy.
	getRequest := &iampb.GetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	rawPolicy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		return nil, err
	}

	member := fmt.Sprintf("serviceAccount:%s", identity.Identifier)
	var result []string
	for _, policy := range rawPolicy.Bindings {
		if slices.Contains(policy.Members, member) {
			result = append(result, policy.Role)
		}
	}

	return &DesiredPermissions{
		Roles: result,
	}, nil
}

// SetIAMBindings implements CloudImpl.
func (g *GCloud) SetPermissions(ctx context.Context, identity *Identity, permissions *DesiredPermissions) error {
	log := logf.FromContext(ctx)
	log.Info("Getting kafka IAM bindings")
	member := fmt.Sprintf("serviceAccount:%s", identity.Identifier)
	// TODO: Remove the copy-pasted code, see the GetIAMBindings
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return err
	}
	defer func(client *resourcemanager.ProjectsClient) {
		if err := client.Close(); err != nil {
			log.Error(err, "Couldn't close the google client")
		}
	}(client)

	// Get the current IAM policy.
	getRequest := &iampb.GetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	rawPolicy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		return err
	}

	updatedPolicy := cleanUpPolicy(ctx, member, rawPolicy)
	// Removing roles from the binding, to make sure that the operator is controlling all permissions
	// TODO: It can lead to access problems, and should be removed once we find a better way to handle obsolete permissions
	log.Info("Cleaning up the bindings")
	setRequestCleanup := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Policy:   updatedPolicy,
	}
	_, err = client.SetIamPolicy(ctx, setRequestCleanup)
	if err != nil {
		log.Error(err, "Failed to set IAM policy")
		return err
	}

	rawPolicyNew, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		return err
	}
	updatedPolicy = cleanUpPolicy(ctx, member, rawPolicyNew)

	for _, role := range permissions.Roles {
		added := false
		for _, binding := range updatedPolicy.Bindings {
			// Always add a readWriteRole, becase the real access is managed by ACLs
			if binding.Role == role {
				log.Info("Adding a new member to a role", "member", identity.Identifier, "role", role)
				binding.Members = append(binding.Members, member)
				added = true
			}
		}
		if !added {
			log.Info("No existing binding found, creating a new one", "role", role)
			updatedPolicy.Bindings = append(updatedPolicy.Bindings, &iampb.Binding{
				Role:    role,
				Members: []string{member},
			})
		}
	}

	log.Info("Updating bindings")
	// Set the updated IAM policy.
	setRequest := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Policy:   updatedPolicy,
	}
	_, err = client.SetIamPolicy(ctx, setRequest)
	if err != nil {
		log.Error(err, "Failed to set IAM policy")
		return err
	}

	return nil
}

func (g *GCloud) EqualPermissions(ctx context.Context, want, have *DesiredPermissions) bool {
	log := logf.FromContext(ctx)
	log.Info("Checking if desired permissions are applied")
	slices.Sort(want.Roles)
	slices.Sort(have.Roles)
	if !reflect.DeepEqual(want, have) {
		return false
	}
	return true
}

func (g *GCloud) DeletePermissions(ctx context.Context, identity *Identity) error {
	log := logf.FromContext(ctx)
	log.Info("Deleting the kafka IAM binding")

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		log.Error(err, "Failed to create client")
		return err
	}
	defer func(client *resourcemanager.ProjectsClient) {
		if err := client.Close(); err != nil {
			log.Error(err, "Couldn't close the google client")
		}
	}(client)

	// Get the current IAM policy.
	getRequest := &iampb.GetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	rawPolicy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		log.Error(err, "Failed to get IAM policy")
		return err
	}
	serviceAccountEmail := identity.Identifier
	updatedPolicy := cleanUpPolicy(ctx, serviceAccountEmail, rawPolicy)

	log.Info("Updating bindings")
	// Set the updated IAM policy.
	setRequest := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + g.ProjectID,
		Policy:   updatedPolicy,
	}
	_, err = client.SetIamPolicy(ctx, setRequest)
	if err != nil {
		log.Error(err, "Failed to set IAM policy")
		return err
	}

	return nil
}

// AddWorkloadIdentityBinding implements CloudImpl.
func (g *GCloud) AddWorkloadIdentity(ctx context.Context, k8sNs, k8sSa string, identityName string) error {
	log := logf.FromContext(ctx).WithValues("cloudSA", identityName, "k8sNs", k8sNs, "k8sSa", k8sSa)
	log.Info("Adding workload identity binding")

	request := &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Bindings: []*iam.Binding{
				{
					Members: []string{
						fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", g.ProjectID, k8sNs, k8sSa),
					},
					Role: "roles/iam.workloadIdentityUser",
				},
			},
		},
	}

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	_, err = service.Projects.ServiceAccounts.SetIamPolicy(identityName, request).Do()
	if err != nil {
		log.Info("Coudln't set the Iam Policy", "error", err)
		return err
	}
	return nil
}

// CheckWorkloadIdentityBinding implements CloudImpl.
func (g *GCloud) CheckWorkloadIdentity(ctx context.Context, k8sNs, k8sSa, identityName string) error {
	log := logf.FromContext(ctx).WithValues("cloudSA", identityName, "k8sNs", k8sNs, "k8sSa", k8sSa)
	log.Info("Checking workload identity binding")
	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	policy, err := service.Projects.ServiceAccounts.GetIamPolicy(identityName).Do()
	if err != nil {
		return err
	}
	for _, binding := range policy.Bindings {
		if binding.Role == "roles/iam.workloadIdentityUser" {
			if slices.Contains(binding.Members, fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", g.ProjectID, k8sNs, k8sSa)) {
				return nil
			}
		}
	}
	return ErrNotFound
}

// GetServiceAccount implements CloudImpl.
func (g *GCloud) GetIdentity(ctx context.Context, name string) (*Identity, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Getting a service account")

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return nil, err
	}

	serviceAccount := &Identity{
		Name:       "",
		Identifier: "",
	}

	// We need either ID or email to get a service account
	serviceAccountEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", name, g.ProjectID)
	log = log.WithValues("email", serviceAccountEmail)
	log.Info("trying to get SA")
	var sa *iam.ServiceAccount
	sa, err = service.Projects.ServiceAccounts.
		Get(fmt.Sprintf("projects/%s/serviceAccounts/%s", g.ProjectID, serviceAccountEmail)).Do()
	if err != nil {
		if errCasted, ok := err.(*googleapi.Error); ok {
			// If doesn't exist
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if errCasted.Code == 404 {
				log.Info("Service Account is not found, skipping")
				return nil, ErrNotFound
			}
		}
		log.Info("Can't get a service account", "error", err)
	}

	serviceAccount.Name = sa.Name
	serviceAccount.Identifier = sa.Email
	return serviceAccount, nil
}

// CreateServiceAccount implements CloudImpl.
func (g *GCloud) CreateIdentity(ctx context.Context, identityName string) (*Identity, error) {
	log := logf.FromContext(ctx).WithValues("name", identityName)
	log.Info("Creating a service account")

	request := &iam.CreateServiceAccountRequest{
		AccountId: identityName,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: identityName,
			Description: "Managed by the kafka user operator",
		},
	}

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return nil, err
	}

	result := &Identity{}
	sa, err := service.Projects.ServiceAccounts.Create("projects/"+g.ProjectID, request).Do()
	if err != nil {
		if errCasted, ok := err.(*googleapi.Error); ok {
			// If already exists
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if errCasted.Code == 409 {
				log.Info("Service Account already exists, re-using")
			}
			result, err = g.GetIdentity(ctx, identityName)
			if err != nil {
				return nil, err
			}
			return result, nil
		} else {
			log.Error(err, "Couldn't create a service account")
			return nil, err
		}
	}
	result.Identifier = sa.Email
	result.Name = sa.Name

	log.Info("Service account is created", "id", result.Identifier)
	return result, nil
}

func (g *GCloud) DeleteIdentity(ctx context.Context, identity *Identity) error {
	serviceAccountEmail := identity.Identifier
	log := logf.FromContext(ctx)
	log.Info("Deleting a service account", "n", serviceAccountEmail)

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	_, err = service.Projects.ServiceAccounts.
		Delete(fmt.Sprintf("projects/%s/serviceAccounts/%s", g.ProjectID, serviceAccountEmail)).Do()
	if err != nil {
		if errCasted, ok := err.(*googleapi.Error); ok {
			// If doesn't exist
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if errCasted.Code == 404 {
				log.Info("Service Account is not found, skipping")
			}
		} else {
			log.Error(err, "Couldn't create a service account")
			return err
		}
	}

	return nil
}

func NewGCloudInstance(projectID, clientRole string) CloudImpl {
	return &GCloud{
		ProjectID:  projectID,
		ClientRole: clientRole,
	}
}

// This function is removing a service account from the policies,
// where the service account exists. It's needed for both updating
// and removing users
func cleanUpPolicy(ctx context.Context, memberName string, policy *iampb.Policy) *iampb.Policy {
	log := logf.FromContext(ctx)
	newPolicy := policy
	for _, binding := range newPolicy.Bindings {
		if slices.Contains(binding.Members, memberName) {
			var newMembers []string
			for _, member := range binding.Members {
				if member != memberName {
					newMembers = append(newMembers, member)
				} else {
					log.Info("Removing member from the policy", "member", memberName, "role", binding.Role)
				}
			}
			binding.Members = newMembers
		}
	}
	return newPolicy
}

func (g *GCloud) GetSAAnnotations(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) map[string]string {
	log := logf.FromContext(ctx)
	log.Info("Getting service account annotations")
	return map[string]string{
		consts.ANNOTATION_GKE_EMAIL: userCR.Status.SAEmail,
	}
}

func (g *GCloud) IsSAReady(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, sa *corev1.ServiceAccount) bool {
	log := logf.FromContext(ctx)
	log.Info("Checking if the service account is ready")
	email, ok := sa.GetAnnotations()[consts.ANNOTATION_GKE_EMAIL]
	return ok && email == userCR.Status.SAEmail
}

func (g *GCloud) CleanupSA(ctx context.Context, sa *corev1.ServiceAccount) {
	log := logf.FromContext(ctx)
	log.Info("Deleting service account annotation")
	if sa.Annotations == nil {
		return
	}
	delete(sa.Annotations, consts.ANNOTATION_GKE_EMAIL)
}

func (g *GCloud) BuildDesiredPermissions(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, allowedRoles []string) (*DesiredPermissions, error) {
	log := logf.FromContext(ctx)
	log.Info("Building a list of desired roles")
	grantedRoles := []string{g.ClientRole}

	if len(userCR.Spec.ExtraRoles) > 0 {
		for _, extraRole := range userCR.Spec.ExtraRoles {
			if !slices.Contains(allowedRoles, extraRole.Name) {
				err := errors.New("extra permission is not allowed")
				errMsg := fmt.Sprintf("%s permissions needs to be added to allowed permissions", extraRole.Name)
				log.Error(err, errMsg)
				continue
			}
			grantedRoles = append(grantedRoles, extraRole.Name)
		}
	}
	return &DesiredPermissions{
		Roles: grantedRoles,
	}, nil
}

func TestCheckCleanupPolicies(t *testing.T) {
	saEmail := "test@test.test"
	policy := &iampb.Policy{
		Version: 0,
		Bindings: []*iampb.Binding{
			{
				Role:    "test1",
				Members: []string{"check@check.check", "test@test.test"},
			},
			{
				Role:    "test2",
				Members: []string{"test@test.test"},
			},
			{
				Role:    "test3",
				Members: []string{"check@check.check"},
			},
		},
	}

	newPolicy := cleanUpPolicy(context.TODO(), saEmail, policy)
	assert.Equal(t, []string{"check@check.check"}, newPolicy.Bindings[0])
	assert.Equal(t, []string{}, newPolicy.Bindings[1])
	assert.Equal(t, []string{"check@check.check"}, newPolicy.Bindings[2])
}
