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
	"os"
	"regexp"
	"strings"
	"time"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/resourcemanager/apiv3"
	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// UserReconciler reconciles a User object
type UserReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=users/finalizers,verbs=update

func (r *UserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	// TODO: Should be configurable
	reconcilePeriod := time.Duration(time.Minute)
	reconcileResult := reconcile.Result{RequeueAfter: reconcilePeriod}
	// TODO(user): your logic here
	projectID := os.Getenv("GCP_PROJECT")
	userCR := &gcpkafkav1alpha1.User{}
	err := r.Get(ctx, req.NamespacedName, userCR)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcileResult, nil
		}
		return reconcileResult, err
	}

	// TODO: Find a way to configure it
	roleName := "testtesttest"
	email, err := createServiceAccount(ctx, userCR, projectID, "testtesttest", roleName)
	if err != nil {
		log.Error(err, "bruh")
	}

	k8sSA := &corev1.ServiceAccount{}

	err = r.Get(ctx, types.NamespacedName{
		Namespace: userCR.GetNamespace(),
		Name:      userCR.Spec.ServiceAccountName,
	}, k8sSA)

	if err != nil {
		return reconcileResult, nil
	}
	k8sSA.Annotations["iam.gke.io/gcp-service-account"] = email

	err = r.Update(ctx, k8sSA)
	if err != nil {
		log.Error(err, "bruh")
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

// createServiceAccount creates a service account.
func createServiceAccount(ctx context.Context, userCR *gcpkafkav1alpha1.User, projectID, name, roleName string) (string, error) {
	log := log.FromContext(ctx)
	service, err := iam.NewService(ctx)
	gcpSAName := fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName())
	gcpSAName = stringSanitize(gcpSAName, 30)

	log.Info("Creating a Service Account")
	if err != nil {
		return "", fmt.Errorf("iam.NewService: %w", err)
	}
	// Create SA
	request := &iam.CreateServiceAccountRequest{
		AccountId: gcpSAName,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: gcpSAName,
			Description: "Managed by the kafka user operator",
		},
	}

	acc, err := service.Projects.ServiceAccounts.Create("projects/"+projectID, request).Do()
	if err != nil {
		log.Info(fmt.Sprintf("lala %v", acc))
		if err, ok := err.(*googleapi.Error); ok {
			log.Info(err.Message)
			// If already exists
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if err.Code == 409 {
				log.Info("Service Account already exists, re-using")
			}
		} else {
			return "", fmt.Errorf("Projects.ServiceAccounts.Create: %w", err)
		}
	}
	var sa *iam.ServiceAccount
	// Get SA
	for i := 0; i < 10; i++ {
		saName := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", gcpSAName, projectID)
		log.Info("trying to get SA", "try", i, "email", saName)
		sa, err = service.Projects.ServiceAccounts.Get(fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, saName)).Do()
		if err != nil {
			log.Error(err, "Can't get a service account")
		} else {
			break
		}
		time.Sleep(time.Second * 15)
	}

	if sa == nil {
		err := errors.New("service account is not found")
		log.Error(err, "couldn't find a service account", "name", gcpSAName)
	}
	log.Info(fmt.Sprintf("%v", sa))
	// Assign kafka policies to the SA

	req3 := &iam.SetIamPolicyRequest{
		Policy: &iam.Policy{
			Bindings: []*iam.Binding{
				{
					Members: []string{
						fmt.Sprintf("serviceAccount:%s.svc.id.goog[%s/%s]", projectID, userCR.GetNamespace(), userCR.Spec.ServiceAccountName),
					},
					Role: "roles/iam.workloadIdentityUser",
				},
			},
		},
	}
	_, err = service.Projects.ServiceAccounts.SetIamPolicy(sa.Name, req3).Do()

	if err != nil {
		return "", fmt.Errorf("Projects.ServiceAccounts.SetIamPolicy: %w", err)
	}

	
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return "", err
	}
	defer client.Close()

	// Get the current IAM policy.
	req := &iampb.GetIamPolicyRequest{
		Resource: "projects/" + projectID,
	}
	policy, err := client.GetIamPolicy(ctx, req)
	if err != nil {
		fmt.Printf("Failed to get IAM policy: %v\n", err)
		return "", err
	}

	log.Info(fmt.Sprintf("%v", policy))

	newBinding := &iampb.Binding{
		Role:    "roles/managedkafka.viewer",
		Members: []string{
						fmt.Sprintf("serviceAccount:%s", sa.Email),
		},
	}
	policy.Bindings = append(policy.Bindings, newBinding)

	// Set the updated IAM policy.
	setReq := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + projectID,
		Policy:   policy,
	}
	updPol, err := client.SetIamPolicy(ctx, setReq)
	if err != nil {
		fmt.Printf("Failed to set IAM policy: %v\n", err)
		return "", nil
	}

	log.Info(fmt.Sprintf("%v", updPol))
	
	return sa.Email, nil
}

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
