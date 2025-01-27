/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3"
	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/genproto/googleapis/type/expr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	READ_ONLY_ACCESS  = "readOnly"
	READ_WRITE_ACCESS = "readWrite"
)

// UserReconciler reconciles a User object
type UserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Opts   *UserReconcilerOpts
}

type UserReconcilerOpts struct {
	// The google project ID
	GoogleProject string
	ReadWriteRole string
	ReadOnlyRole  string
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=watch;update;list

func (r *UserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	// TODO: Should be configurable
	reconcilePeriod := time.Duration(time.Minute)
	reconcileResult := reconcile.Result{RequeueAfter: reconcilePeriod}

	userCR := &gcpkafkav1alpha1.User{}
	err := r.Get(ctx, req.NamespacedName, userCR)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcileResult, nil
		}
		return reconcileResult, err
	}

	if userCR.DeletionTimestamp != nil {
		return reconcileResult, nil
	}

	if err := r.createOrUpdate(ctx, userCR); err != nil {
		log.Error(err, "Reconciliation failed")
		return reconcileResult, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *UserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gcpkafkav1alpha1.User{}).
		Named("user").
		Complete(r)
}

// Handle cases when resource is created or updated
func (r *UserReconciler) createOrUpdate(ctx context.Context, userCR *gcpkafkav1alpha1.User) error {
	log := log.FromContext(ctx)
	log.Info("Handle resource creation/update")
	// Service accounts have limited name lenght, we must make sure
	// that we're not exceeding the limit
	gcpServiceAccountName := stringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	// Trying to create a service account
	if err := createServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName); err != nil {
		log.Error(err, "Couldn't create a service account")
		return err
	}

	sa, err := getServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName)
	if err != nil {
		log.Error(err, "Couldn't get a service account")
		return err
	}

	k8sServiceAccountName := fmt.Sprintf("%s/%s", userCR.GetNamespace(), userCR.Spec.ServiceAccountName)
	if err := addWorkloadIdentityBinding(ctx, r.Opts.GoogleProject, k8sServiceAccountName, sa.Name); err != nil {
		log.Error(err, "Couldn't add a workload identity binding")
		return err
	}

	if err := addKafkaIAMBinding(ctx, userCR.Spec.TopicAccess, userCR.Spec.ClusterAccess, r.Opts.GoogleProject, r.Opts.ReadOnlyRole, r.Opts.ReadWriteRole, sa.Email); err != nil {
		log.Error(err, "Couldn't add a kafka binding to the project")
		return err
	}

	k8sSA := &corev1.ServiceAccount{}

	err = r.Get(ctx, types.NamespacedName{
		Namespace: userCR.GetNamespace(),
		Name:      userCR.Spec.ServiceAccountName,
	}, k8sSA)

	if err != nil {
		return err
	}
	k8sSA.Annotations["iam.gke.io/gcp-service-account"] = sa.Email

	err = r.Update(ctx, k8sSA)
	if err != nil {
		log.Error(err, "Couldn't annotate a service account", "name", k8sSA.GetName())
		return err
	}
	userCR.Status.Ready = true
	return nil
}

func createServiceAccount(ctx context.Context, projectID, serviceAccountName string) error {
	log := log.FromContext(ctx)
	log.Info("Creating a service account", "name", serviceAccountName)
	request := &iam.CreateServiceAccountRequest{
		AccountId: serviceAccountName,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: serviceAccountName,
			Description: "Managed by the kafka user operator",
		},
	}

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	_, err = service.Projects.ServiceAccounts.Create("projects/"+projectID, request).Do()
	if err != nil {
		if err, ok := err.(*googleapi.Error); ok {
			// If already exists
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if err.Code == 409 {
				log.Info("Service Account already exists, re-using")
			}
		} else {
			log.Error(err, "Couldn't create a service account")
			return err
		}
	}

	return nil
}

func getServiceAccount(ctx context.Context, projectID, serviceAccountName string) (*iam.ServiceAccount, error) {
	log := log.FromContext(ctx)
	log.Info("Getting a service account", "name", serviceAccountName)
	var sa *iam.ServiceAccount

	var err error
	var service *iam.Service

	service, err = iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return nil, err
	}

	// Get SA
	for i := 0; i < 10; i++ {
		// We need either ID or email to get a service account
		serviceAccountEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccountName, projectID)
		log.Info("trying to get SA", "try", i, "email", serviceAccountEmail)

		sa, err = service.Projects.ServiceAccounts.
			Get(fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, serviceAccountEmail)).Do()

		if err != nil {
			log.Error(err, "Can't get a service account")
			time.Sleep(time.Second * 15)
		} else {
			break
		}
	}

	if err != nil {
		log.Error(err, "couldn't find a service account", "name", serviceAccountName)
		return nil, err
	}

	return sa, nil
}

func addWorkloadIdentityBinding(
	ctx context.Context,
	projectID, k8sServiceAccountName, gcpServiceAccountName string,
) error {
	log := log.FromContext(ctx)
	log.Info("Adding a workload identity role to the k8s service account", "name", k8sServiceAccountName)

	request := &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Bindings: []*iam.Binding{
				{
					Members: []string{
						fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s]", projectID, k8sServiceAccountName),
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
	_, err = service.Projects.ServiceAccounts.SetIamPolicy(gcpServiceAccountName, request).Do()

	if err != nil {
		log.Error(err, "Couldn't add a workload identity binding",
			"google service account", gcpServiceAccountName,
			"k8s service account", k8sServiceAccountName,
		)
		return err
	}

	return nil
}

func addKafkaIAMBinding(
	ctx context.Context,
	topicAccess []*gcpkafkav1alpha1.TopicAccess,
	clusterAccess string,
	projectID, readOnlyRole, readWriteRole, serviceAccountEmail string,
) error {
	log := log.FromContext(ctx)
	log.Info("Adding the kafka IAM binding")

	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		log.Error(err, "Failed to create client")
		return err
	}
	defer client.Close()

	// Get the current IAM policy.
	getRequest := &iampb.GetIamPolicyRequest{
		Resource: "projects/" + projectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	policy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		log.Error(err, "Failed to get IAM policy")
		return err
	}

	// Remove all the roles to ensure the changed roles
	i := 0 // output index
	for _, binding := range policy.Bindings {
		if !slices.Contains(binding.Members, fmt.Sprintf("serviceAccount:%s", serviceAccountEmail)) {
			policy.Bindings[i] = binding
			i++
		} else {
			log.Info("Removing binding", "binding", binding.String())
		}
	}
	for j := i; j < len(policy.Bindings); j++ {
		policy.Bindings[j] = nil
	}
	policy.Bindings = policy.Bindings[:i]
	log.Info("Updating bindings")
	// If not clusterAccess, we need to build policies
	if len(clusterAccess) == 0 {
		var readOnlyTopics, readWriteTopics string
		for _, access := range topicAccess {
			switch access.Role {
			case READ_ONLY_ACCESS:
				if len(readOnlyTopics) == 0 {
					readOnlyTopics += fmt.Sprintf("resource.name.endsWith('%s')", access.Topic)
				} else {
					readOnlyTopics += fmt.Sprintf("|| resource.name.endsWith('%s')", access.Topic)
				}
			case READ_WRITE_ACCESS:
				if len(readWriteTopics) == 0 {
					readWriteTopics += fmt.Sprintf("resource.name.endsWith('%s')", access.Topic)
				} else {
					readWriteTopics += fmt.Sprintf("|| resource.name.endsWith('%s')", access.Topic)
				}
			default:
				err := errors.New("Unsupported access type")
				log.Error(err, "Unsupported access type", "type", access.Role)
				return err
			}
		}

		if len(readWriteTopics) > 0 {
			newBinding := &iampb.Binding{
				Role: readWriteRole,
				Members: []string{
					fmt.Sprintf("serviceAccount:%s", serviceAccountEmail),
				},
				Condition: &expr.Expr{
					Title:      "Read Write kafka Access",
					Expression: readWriteTopics,
				},
			}
			policy.Bindings = append(policy.Bindings, newBinding)
		}
		if len(readOnlyTopics) > 0 {
			newBinding := &iampb.Binding{
				Role: readOnlyRole,
				Members: []string{
					fmt.Sprintf("serviceAccount:%s", serviceAccountEmail),
				},
				Condition: &expr.Expr{
					Title:      "Read Only kafka Access",
					Expression: readOnlyRole,
				},
			}
			policy.Bindings = append(policy.Bindings, newBinding)
		}
	} else {
		var clusterRole string
		switch clusterAccess {
		case READ_ONLY_ACCESS:
			clusterRole = readOnlyRole
		case READ_WRITE_ACCESS:
			clusterRole = readWriteRole
		default:
			err := errors.New("Unsupported access type")
			log.Error(err, "Unsupported access type", "type", clusterAccess)
			return err
		}
		newBinding := &iampb.Binding{
			Role: clusterRole,
			Members: []string{
				fmt.Sprintf("serviceAccount:%s", serviceAccountEmail),
			},
		}
		policy.Bindings = append(policy.Bindings, newBinding)
	}

	// Set the updated IAM policy.
	setRequest := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + projectID,
		Policy:   policy,
	}
	_, err = client.SetIamPolicy(ctx, setRequest)
	if err != nil {
		log.Error(err, "Failed to set IAM policy")
		return err
	}

	return nil
}

// TODO: Move to another package and add unit tests
// StringSanitize sanitizes and truncates a string to a fixed length using a hash function.
// useful for restricting the length and content of user supplied database identifiers.
func stringSanitize(s string, limit int) string {
	// use lowercase exclusively for identifiers.
	// https://dev.mysql.com/doc/refman/5.7/en/identifier-case-sensitivity.html
	s = strings.ToLower(s)

	// Strip out any unsupported characters.
	// https://dev.mysql.com/doc/refman/5.7/en/identifiers.html
	unsupportedChars := regexp.MustCompile(`[^0-9a-zA-Z$-]`)
	s = unsupportedChars.ReplaceAllString(s, "-")

	if len(s) <= limit {
		return s
	}

	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(s)))

	if limit <= 9 {
		return hash[:limit]
	}

	return fmt.Sprintf("%s-%s", s[:limit-9], hash[:8])
}
