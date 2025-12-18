package cloud

import (
	"context"
	"fmt"
	"slices"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iam/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

type GCloud struct {
	ProjectID string
}

// GetIAMBindings implements CloudImpl.
// TODO: It should be possible to use the googleapi lib
func (g *GCloud) GetIAMBindings(ctx context.Context, cloudSaID string) ([]string, error) {
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

	member := fmt.Sprintf("serviceAccount:%s", cloudSaID)
	var result []string
	for _, policy := range rawPolicy.Bindings {
		if slices.Contains(policy.Members, member) {
			result = append(result, policy.Role)
		}
	}

	return result, nil
}

// SetIAMBindings implements CloudImpl.
func (g *GCloud) SetIAMBindings(ctx context.Context, cloudSaID string, roles []string) error {
	log := logf.FromContext(ctx)
	log.Info("Getting kafka IAM bindings")
	member := fmt.Sprintf("serviceAccount:%s", cloudSaID)
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

	for _, role := range roles {
		added := false
		for _, binding := range updatedPolicy.Bindings {
			// Always add a readWriteRole, becase the real access is managed by ACLs
			if binding.Role == role {
				log.Info("Adding a new member to a role", "member", cloudSaID, "role", role)
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

// AddWorkloadIdentityBinding implements CloudImpl.
func (g *GCloud) AddWorkloadIdentityBinding(ctx context.Context, k8sSa string, cloudSa string) error {
	log := logf.FromContext(ctx).WithValues("cloudSA", cloudSa, "k8sSa", k8sSa)
	log.Info("Adding workload identity binding")

	request := &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Bindings: []*iam.Binding{
				{
					Members: []string{
						fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s]", g.ProjectID, k8sSa),
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
	_, err = service.Projects.ServiceAccounts.SetIamPolicy(cloudSa, request).Do()
	if err != nil {
		log.Info("Coudln't set the Iam Policy", "error", err)
		return err
	}
	return nil
}

// CheckWorkloadIdentityBinding implements CloudImpl.
func (g *GCloud) CheckWorkloadIdentityBinding(ctx context.Context, k8sSa string, cloudSa string) error {
	log := logf.FromContext(ctx).WithValues("cloudSA", cloudSa, "k8sSa", k8sSa)
	log.Info("Checking workload identity binding")
	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	policy, err := service.Projects.ServiceAccounts.GetIamPolicy(cloudSa).Do()
	if err != nil {
		return err
	}
	for _, binding := range policy.Bindings {
		if binding.Role == "roles/iam.workloadIdentityUser" {
			if slices.Contains(binding.Members, fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s]", g.ProjectID, k8sSa)) {
				return nil
			}
		}
	}

	return ErrNotFound
}

// GetServiceAccount implements CloudImpl.
func (g *GCloud) GetServiceAccount(ctx context.Context, name string) (*ServiceAccount, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Getting a service account")

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return nil, err
	}

	serviceAccount := &ServiceAccount{
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
func (g *GCloud) CreateServiceAccount(ctx context.Context, name string) (*ServiceAccount, error) {
	log := logf.FromContext(ctx).WithValues("name", name)
	log.Info("Creating a service account")

	request := &iam.CreateServiceAccountRequest{
		AccountId: name,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: name,
			Description: "Managed by the kafka user operator",
		},
	}

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return nil, err
	}

	result := &ServiceAccount{}
	sa, err := service.Projects.ServiceAccounts.Create("projects/"+g.ProjectID, request).Do()
	if err != nil {
		if errCasted, ok := err.(*googleapi.Error); ok {
			// If already exists
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if errCasted.Code == 409 {
				log.Info("Service Account already exists, re-using")
			}
			result, err = g.GetServiceAccount(ctx, name)
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

func NewGCloudInstance(projectID string) CloudImpl {
	return &GCloud{
		ProjectID: projectID,
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
