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
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	kafkawrap "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// KafkaUserReconciler reconciles a User object
type KafkaUserReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Opts     *KafkaUserReconcilerOpts
	Recorder record.EventRecorder
}

type KafkaUserReconcilerOpts struct {
	// The google project ID
	GoogleProject   string
	ClientRole      string
	KafkaInstance   kafkawrap.KafkaImpl
	AdminUserEmail  string
	ReconcilePeriod time.Duration
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=watch;update;list

func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	reconcileResultRepeat := reconcile.Result{RequeueAfter: r.Opts.ReconcilePeriod, Requeue: true}
	reconcileResultNoRepeat := reconcile.Result{Requeue: false}

	// Get the object from the k8s api
	userCR := &gcpkafkav1alpha1.KafkaUser{}
	err := r.Get(ctx, req.NamespacedName, userCR)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcileResultNoRepeat, nil
		}
		log.Error(err, "Could't get a kafka user object")
		return reconcileResultRepeat, err
	}

	if userCR.Status.ReconciliationActive {
		log.Info("Another reconciliation is in progress, skipping ...")
		return reconcileResultNoRepeat, nil
	}

	if userCR.DeletionTimestamp != nil {
		if err := r.delete(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		return ctrl.Result{}, nil
	}

	hash, err := getSpecHash(userCR.Spec.DeepCopy())
	if err != nil {
		return reconcileResultRepeat, nil
	}
	isReconcile := false
	if userCR.Status.ConfigHash != hash {
		log.Info("Spec was changed", "oldHash", userCR.Status.ConfigHash, "newHash", hash)
		isReconcile = true
	}

	// Update object status always when function exit abnormally or through a panic.
	if isReconcile {
		userCR.Status.ConfigHash = hash
		userCR.Status.Ready = false
		userCR.Status.ReconciliationActive = true
		defer func() {
			userCR.Status.ReconciliationActive = false
			if err := r.updateObject(ctx, userCR); err != nil {
				log.Error(err, "Couldn't update an object on defer")
			}
		}()

		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}

		log.Info("User is not ready")
		if err := r.createOrUpdate(ctx, userCR); err != nil {
			log.Error(err, "Reconciliation failed")
			return reconcileResultRepeat, err
		}
		if err := r.updateStatus(ctx, userCR); err != nil {
			log.Error(err, "Failed to update the user status")
		}
		if err := r.updateObject(ctx, userCR); err != nil {
			log.Error(err, "Failed to update the user object")
		}
	}

	log.Info("Reconciliation is successful")
	return reconcileResultNoRepeat, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *KafkaUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gcpkafkav1alpha1.KafkaUser{}).
		Named("user").
		Watches(
			&corev1.ServiceAccount{},
			handler.EnqueueRequestsFromMapFunc(r.findKafkaUserForServiceAccount),
		).
		Complete(r)
}

func (r *KafkaUserReconciler) findKafkaUserForServiceAccount(ctx context.Context, sa client.Object) []reconcile.Request {
	log := log.FromContext(ctx)
	name := sa.GetName()
	namespace := sa.GetNamespace()
	log = log.WithValues("sa", name, "namespace", namespace)
	log.Info("A service account modification was spotted")

	kafkaUsers := &gcpkafkav1alpha1.KafkaUserList{}
	if err := r.List(ctx, kafkaUsers, &client.ListOptions{Namespace: namespace}); err != nil {
		log.Error(err, "Couldn't list kafka users")
		return nil
	}

	for _, user := range kafkaUsers.Items {
		if user.Spec.ServiceAccountName == name {
			user.Status.Ready = false
			if err := r.updateStatus(ctx, &user); err != nil {
				log.Error(err, "Couldn't update kafka user status")
			}
		}
	}

	return []reconcile.Request{}
}

// Handle cases when resource is created or updated
func (r *KafkaUserReconciler) createOrUpdate(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := log.FromContext(ctx)
	log.Info("Handle resource creation/update")
	// Service accounts have limited name length, we must make sure
	// that we're not exceeding the limit
	gcpServiceAccountName := stringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	// Trying to create a service account
	if err := createServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName); err != nil {
		errMsg := "Couldn't create a GCP service account"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}

	userCR.SetFinalizers(helpers.SliceAppendIfMissing(
		userCR.GetFinalizers(),
		consts.GCP_SERVICE_ACCOUNT_FINALIZER,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	sa, err := getServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName, 10)
	if err != nil {
		errMsg := "Couldn't get a GCP service account"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}

	userCR.Status.SAEmail = sa.Email
	if err := r.updateStatus(ctx, userCR); err != nil {
		return err
	}

	k8sServiceAccountName := fmt.Sprintf("%s/%s", userCR.GetNamespace(), userCR.Spec.ServiceAccountName)
	if err := addWorkloadIdentityBinding(ctx, r.Opts.GoogleProject, k8sServiceAccountName, sa.Name); err != nil {
		errMsg := "Couldn't add a workload identity binding"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}

	if err := addKafkaIAMBinding(ctx, r.Opts.GoogleProject, r.Opts.ClientRole, sa.Email); err != nil {
		errMsg := "Couldn't add a kafka binding to the project"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}

	userCR.SetFinalizers(helpers.SliceAppendIfMissing(
		userCR.GetFinalizers(),
		consts.KAFKA_IAM_BINDING_FINALIZER,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	k8sSA := &corev1.ServiceAccount{}

	err = r.Get(ctx, types.NamespacedName{
		Namespace: userCR.GetNamespace(),
		Name:      userCR.Spec.ServiceAccountName,
	}, k8sSA)
	if err != nil {
		errMsg := "Couldn't get a k8s service account, make sure it's created"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}
	if k8sSA.Annotations == nil {
		k8sSA.Annotations = map[string]string{}
	}
	k8sSA.Annotations["iam.gke.io/gcp-service-account"] = sa.Email

	err = r.Update(ctx, k8sSA)
	if err != nil {
		errMsg := "Couldn't annotate a service account"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg, "name", k8sSA.GetName())
		return err
	}

	userCR.SetFinalizers(helpers.SliceAppendIfMissing(
		userCR.GetFinalizers(),
		consts.K8S_SERVICE_ACCOUNT_FINALIZER,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	if err := r.updateACLs(ctx, userCR); err != nil {
		errMsg := "Couldn't update ACLs"
		r.Recorder.Event(userCR, "Warning", "ReconciliationError", errMsg)
		log.Error(err, errMsg)
		return err
	}

	userCR.SetFinalizers(helpers.SliceAppendIfMissing(
		userCR.GetFinalizers(),
		consts.KAFKA_ACLS_FINALIZER,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	userCR.Status.Ready = true
	if err := r.updateStatus(ctx, userCR); err != nil {
		return err
	}
	return nil
}

// Handle cases when resource is deleted
func (r *KafkaUserReconciler) delete(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := log.FromContext(ctx)
	log.Info("Handle resource deletion")
	// Service accounts have limited name length, we must make sure
	// that we're not exceeding the limit
	gcpServiceAccountName := stringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	sa, err := getServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName, 1)
	if err != nil {
		log.Info("Couldn't get a service account, continuing", "error", err)
	} else {
		if err := deleteServiceAccount(ctx, r.Opts.GoogleProject, sa.Email); err != nil {
			log.Error(err, "Couldn't create a service account")
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.GCP_SERVICE_ACCOUNT_FINALIZER,
	))
	if err := deleteKafkaIAMBinding(ctx, r.Opts.GoogleProject, userCR.Status.SAEmail); err != nil {
		log.Error(err, "Couldn't add a kafka binding to the project")
		return err
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.KAFKA_IAM_BINDING_FINALIZER,
	))

	k8sSA := &corev1.ServiceAccount{}

	err = r.Get(ctx, types.NamespacedName{
		Namespace: userCR.GetNamespace(),
		Name:      userCR.Spec.ServiceAccountName,
	}, k8sSA)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			log.Info("Service account is not found, continuing...", "sa", userCR.Spec.ServiceAccountName)
		} else {
			return err
		}
	} else {
		delete(k8sSA.Annotations, "iam.gke.io/gcp-service-account")
		err = r.Update(ctx, k8sSA)
		if err != nil {
			log.Error(err, "Couldn't annotate a service account", "name", k8sSA.GetName())
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.K8S_SERVICE_ACCOUNT_FINALIZER,
	))

	if len(userCR.Spec.ClusterAccess) == 0 {
		if err := r.updateACLs(ctx, userCR); err != nil {
			log.Error(err, "Couldn't update ACLs")
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.KAFKA_ACLS_FINALIZER,
	))

	return nil
}

func castAccessToKafkaFormat(ctx context.Context, input []*gcpkafkav1alpha1.TopicAccess) (result []*kafkawrap.TopicAccess, err error) {
	log := log.FromContext(ctx)
	for _, role := range input {
		var castedRole *kafkawrap.TopicAccess
		castedRole, err = kafkawrap.NewTopicAccess(role.Topic, role.Role)
		if err != nil {
			log.Error(err, "Couldn't prepare topic access")
			return
		}
		result = append(result, castedRole)
	}
	return
}

// This functions checks whether an element of the first array
// is presented in the second, and if so, it's not including
// it in the result
func findAccessDiff(first, second []*kafkawrap.TopicAccess) (result []*kafkawrap.TopicAccess) {
	for _, el1 := range first {
		presereve := true
		for _, el2 := range second {
			if el2.Topic == el1.Topic && el2.Operation == el1.Operation {
				presereve = false
				break
			}
		}
		if presereve {
			result = append(result, el1)
		}
	}
	return
}

func (r *KafkaUserReconciler) updateACLs(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) (err error) {
	log := log.FromContext(ctx)
	var desiredAccess []*kafkawrap.TopicAccess

	if len(userCR.Spec.ClusterAccess) > 0 {
		topics, err := r.Opts.KafkaInstance.ListTopics(ctx, true)
		if err != nil {
			return err
		}
		log.Info("Got all the topics from the kafka", "amount", len(topics))
		for _, topic := range topics {
			access, err := kafkawrap.NewTopicAccess(topic, userCR.Spec.ClusterAccess)
			if err != nil {
				return err
			}
			desiredAccess = append(desiredAccess, access)
		}
	} else {
		desiredAccess, err = castAccessToKafkaFormat(ctx, userCR.Spec.TopicAccess)
		if err != nil {
			return err
		}
		log.Info("Desired amount of ACLs", "amount", len(desiredAccess))
	}

	// Append the operator user to every topic, so it doesn't lose access
	for _, topic := range desiredAccess {
		if err := r.Opts.KafkaInstance.CreateACL(ctx, r.Opts.AdminUserEmail, []*kafkawrap.TopicAccess{{
			Topic:     topic.Topic,
			Operation: kafka.ACLOperationAll,
		}}); err != nil {
			return err
		}
	}

	currentAccess, err := castAccessToKafkaFormat(ctx, userCR.Status.TopicAccessApplied)
	if err != nil {
		return err
	}

	log.Info("Current amount of ACLs", "amount", len(currentAccess))
	delAccess := findAccessDiff(currentAccess, desiredAccess)
	log.Info("ACLs are marked for removing", "amount", len(delAccess))
	newAccess := findAccessDiff(desiredAccess, currentAccess)
	log.Info("ACLs are marked for creating", "amount", len(newAccess))

	if len(delAccess) > 0 {
		if err := r.Opts.KafkaInstance.DeleteACL(ctx, userCR.Status.SAEmail, delAccess); err != nil {
			log.Error(err, "Couldn't delete ACLs")
			return err
		}
	}
	if len(newAccess) > 0 {
		if err := r.Opts.KafkaInstance.CreateACL(ctx, userCR.Status.SAEmail, newAccess); err != nil {
			log.Error(err, "Couldn't create ACLs")
			return err
		}
	}

	appliedTopics := []*gcpkafkav1alpha1.TopicAccess{}
	for _, access := range desiredAccess {
		topic, role, err := kafkawrap.ParseTopicAccess(access)
		if err != nil {
			return err
		}
		status := gcpkafkav1alpha1.TopicAccess{
			Topic: topic,
			Role:  role,
		}
		appliedTopics = append(appliedTopics, &status)
	}
	userCR.Status.TopicAccessApplied = appliedTopics
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
		if errCasted, ok := err.(*googleapi.Error); ok {
			// If already exists
			// https://cloud.google.com/pubsub/docs/reference/error-codes
			if errCasted.Code == 409 {
				log.Info("Service Account already exists, re-using")
			}
		} else {
			log.Error(err, "Couldn't create a service account")
			return err
		}
	}

	return nil
}

func deleteServiceAccount(ctx context.Context, projectID, serviceAccountEmail string) error {
	log := log.FromContext(ctx)
	log.Info("Deleting a service account", "name", serviceAccountEmail)

	service, err := iam.NewService(ctx)
	if err != nil {
		log.Error(err, "Couldn't initialize the IAM service")
		return err
	}
	_, err = service.Projects.ServiceAccounts.
		Delete(fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, serviceAccountEmail)).Do()
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

func getServiceAccount(ctx context.Context, projectID, serviceAccountName string, attempts int) (*iam.ServiceAccount, error) {
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
	for i := range attempts {
		// We need either ID or email to get a service account
		serviceAccountEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccountName, projectID)
		log.Info("trying to get SA", "try", i, "email", serviceAccountEmail)

		sa, err = service.Projects.ServiceAccounts.
			Get(fmt.Sprintf("projects/%s/serviceAccounts/%s", projectID, serviceAccountEmail)).Do()

		if err != nil {
			log.Info("Can't get a service account", "error", err)
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
	projectID, clientRole, serviceAccountEmail string,
) error {
	log := log.FromContext(ctx)
	log.Info("Adding the kafka IAM binding")

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
		Resource: "projects/" + projectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	rawPolicy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		log.Error(err, "Failed to get IAM policy")
		return err
	}
	updatedPolicy := cleanUpPolicy(ctx, serviceAccountEmail, rawPolicy)

	member := fmt.Sprintf("serviceAccount:%s", serviceAccountEmail)
	added := false

	for _, binding := range updatedPolicy.Bindings {
		// Always add a readWriteRole, becase the real access is not managed by ACS
		if binding.Role == clientRole {
			log.Info("Adding a new member to a role", "member", serviceAccountEmail, "role", clientRole)
			binding.Members = append(binding.Members, member)
			added = true
		}
	}
	if !added {
		log.Info("No existing binding found, creating a new one", "role", clientRole)
		updatedPolicy.Bindings = append(updatedPolicy.Bindings, &iampb.Binding{
			Role:    clientRole,
			Members: []string{member},
		})
	}

	log.Info("Updating bindings")
	// Set the updated IAM policy.
	setRequest := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + projectID,
		Policy:   updatedPolicy,
	}
	_, err = client.SetIamPolicy(ctx, setRequest)
	if err != nil {
		log.Error(err, "Failed to set IAM policy")
		return err
	}

	return nil
}

// This function is removing a service account from the policies,
// where the service account exists. It's needed for both updating
// and removing users
func cleanUpPolicy(ctx context.Context, saEmail string, policy *iampb.Policy) *iampb.Policy {
	log := log.FromContext(ctx)
	newPolicy := policy
	for _, binding := range newPolicy.Bindings {
		sa := fmt.Sprintf("serviceAccount:%s", saEmail)
		if slices.Contains(binding.Members, saEmail) {
			var newMembers []string
			for _, member := range binding.Members {
				if member != saEmail {
					newMembers = append(newMembers, member)
				} else {
					log.Info("Removing member from the policy", "email", sa, "role", binding.Role)
				}
			}
			binding.Members = newMembers
		}
	}
	return newPolicy
}

func deleteKafkaIAMBinding(ctx context.Context, projectID, serviceAccountEmail string) error {
	log := log.FromContext(ctx)
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
		Resource: "projects/" + projectID,
		Options: &iampb.GetPolicyOptions{
			RequestedPolicyVersion: 3,
		},
	}

	rawPolicy, err := client.GetIamPolicy(ctx, getRequest)
	if err != nil {
		log.Error(err, "Failed to get IAM policy")
		return err
	}
	updatedPolicy := cleanUpPolicy(ctx, serviceAccountEmail, rawPolicy)

	log.Info("Updating bindings")
	// Set the updated IAM policy.
	setRequest := &iampb.SetIamPolicyRequest{
		Resource: "projects/" + projectID,
		Policy:   updatedPolicy,
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

func (r *KafkaUserReconciler) updateStatus(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := log.FromContext(ctx)
	if err := r.Status().Update(ctx, userCR); err != nil {
		log.Error(err, "failed to update status")
		return err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(userCR), userCR); err != nil {
		log.Error(err, "Failed to get an updated object")
		return err
	}
	return nil
}

func (r *KafkaUserReconciler) updateObject(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := log.FromContext(ctx)
	if err := r.Update(ctx, userCR); err != nil {
		log.Error(err, "failed to update status")
		return err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(userCR), userCR); err != nil {
		log.Error(err, "Failed to get an updated object")
		return err
	}
	return nil
}

func getSpecHash(userSpec *gcpkafkav1alpha1.KafkaUserSpec) (string, error) {
	yaml, err := yaml.Marshal(userSpec)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(yaml)), nil
}
