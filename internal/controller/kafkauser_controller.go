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
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/cloud"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	kafkawrap "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
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
	GoogleProject               string
	ClientRole                  string
	KafkaInstance               kafkawrap.KafkaImpl
	CloudInstance               cloud.CloudImpl
	AdminUserEmail              string
	ReconcilePeriod             time.Duration
	ExtraPermissionsCMNamespace string
	ExtraPermissionsCM          string
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=watch;update;list
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=watch;list;get
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
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
		return ctrl.Result{}, err
	}

	if userCR.DeletionTimestamp != nil {
		if err := r.delete(ctx, userCR); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Create a service account in the cloud provider
	cloudSAName := helpers.StringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	// Init the object if it's nil to avoid panics
	if userCR.Status.KafkaUserState == nil {
		userCR.Status.KafkaUserState = &gcpkafkav1alpha1.KafkaUserState{}
		if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
			return reconcileResultRepeat, errUpdate
		}
		return reconcileResultNoRepeat, nil
	}
	if userCR.Status.KafkaUserState.CloudSA {
		log.Info("Cloud service account was created, checking")
		// If SA is not created set the status.cloudSA to false and return
		_, err = r.Opts.CloudInstance.GetServiceAccount(ctx, cloudSAName)
		if err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				userCR.Status.KafkaUserState.CloudSA = false
				userCR.Status.Ready = false
				if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
					return reconcileResultRepeat, errUpdate
				}
				return reconcileResultNoRepeat, nil
			} else {
				return reconcileResultNoRepeat, nil
			}
		}
	} else {
		sa, err := r.Opts.CloudInstance.CreateServiceAccount(ctx, cloudSAName)
		log.Info("Creating a service account")
		if err != nil {
			log.Error(err, "Couldn't create a cloud service account")
			return reconcileResultRepeat, nil
		}
		userCR.SetFinalizers(helpers.SliceAppendIfMissing(
			userCR.GetFinalizers(),
			consts.FINALIZER_CLOUD_SERVICE_ACCOUNT,
		))
		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.Status.KafkaUserState.CloudSA = true
		userCR.Status.SAEmail = sa.Identifier
		userCR.Status.Error = ""
		if err := r.updateStatus(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
	}

	k8sSA := fmt.Sprintf("%s/%s", userCR.GetNamespace(), userCR.Spec.ServiceAccountName)
	cloudSa, err := r.Opts.CloudInstance.GetServiceAccount(ctx, cloudSAName)
	if err != nil {
		return reconcileResultRepeat, err
	}
	if userCR.Status.KafkaUserState.WorkloadIdentity {
		log.Info("Workload identity was added, checking")
		// If Workload Identity is not applied, set the state to false and return
		if err := r.Opts.CloudInstance.CheckWorkloadIdentityBinding(ctx, k8sSA, cloudSa.Name); err != nil {
			if errors.Is(err, cloud.ErrNotFound) {
				userCR.Status.KafkaUserState.WorkloadIdentity = false
				userCR.Status.Ready = false
				if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
					return reconcileResultRepeat, errUpdate
				}
				return reconcileResultNoRepeat, nil
			} else {
				return reconcileResultNoRepeat, nil
			}
		}
	} else {
		log.Info("Adding workload identity")
		if err := r.Opts.CloudInstance.AddWorkloadIdentityBinding(ctx, k8sSA, cloudSa.Name); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.SetFinalizers(helpers.SliceAppendIfMissing(
			userCR.GetFinalizers(),
			consts.FINALIZER_CLOUD_WORKLOAD_IDENTITY,
		))
		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.Status.KafkaUserState.WorkloadIdentity = true
		if err := r.updateStatus(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		return reconcileResultNoRepeat, nil
	}

	// Prepare granted roles (Currently GCP only)
	// TODO: Re-design to make it AWS compatible
	grantedRoles := []string{r.Opts.ClientRole}
	if len(userCR.Spec.ExtraRoles) > 0 {
		allowedRoles, err := r.getAllowedPermissions(ctx, userCR.GetName(), userCR.GetNamespace())
		if err != nil {
			return reconcileResultRepeat, nil
		}
		for _, extraRoles := range userCR.Spec.ExtraRoles {
			log.Info("Checking if the extra permission is allowed for the user")
			if !slices.Contains(allowedRoles, extraRoles) {
				err := errors.New("extra permission is not allowed")
				errMsg := fmt.Sprintf("%s permissions needs to be added to allowed permissions", extraRoles)
				r.Recorder.Event(userCR, corev1.EventTypeWarning, "Error", errMsg)
				log.Error(err, errMsg)
				return reconcileResultRepeat, nil
			}
			grantedRoles = append(grantedRoles, extraRoles)
		}
	}

	if userCR.Status.KafkaUserState.IamBindings {
		log.Info("IAM bindings were added, checking", "wanted", grantedRoles)
		roles, err := r.Opts.CloudInstance.GetIAMBindings(ctx, userCR.Status.SAEmail)
		if err != nil {
			return reconcileResultRepeat, err
		}
		slices.Sort(roles)
		slices.Sort(grantedRoles)
		if !reflect.DeepEqual(roles, grantedRoles) {
			log.Info("Roles don't match", "applied", roles, "desired", grantedRoles)
			userCR.Status.KafkaUserState.IamBindings = false
			userCR.Status.Ready = false
			if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
				return reconcileResultRepeat, errUpdate
			}
			return reconcileResultNoRepeat, nil
		}
	} else {
		log.Info("Adding IAM bindings", "wanted", grantedRoles)
		if err := r.Opts.CloudInstance.SetIAMBindings(ctx, userCR.Status.SAEmail, grantedRoles); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.SetFinalizers(helpers.SliceAppendIfMissing(
			userCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_IAM_BINDING,
		))
		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.Status.KafkaUserState.IamBindings = true
		if err := r.updateStatus(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		return reconcileResultNoRepeat, nil
	}

	// Update the k8s service account
	// TODO: Add a check for the annotation
	if !userCR.Status.KafkaUserState.K8sSA {
		log.Info("Updating the k8s service account")
		k8sSA := &corev1.ServiceAccount{}

		err = r.Get(ctx, types.NamespacedName{
			Namespace: userCR.GetNamespace(),
			Name:      userCR.Spec.ServiceAccountName,
		}, k8sSA)
		if err != nil {
			errMsg := "Could not get a k8s service account, make sure it is created"
			r.Recorder.Event(userCR, corev1.EventTypeWarning, "Error", errMsg)
			userCR.Status.Error = errMsg
			log.Error(err, errMsg)
			return reconcileResultRepeat, err
		}
		if k8sSA.Annotations == nil {
			k8sSA.Annotations = map[string]string{}
		}
		k8sSA.Annotations[consts.ANNOTATION_GKE_EMAIL] = userCR.Status.SAEmail

		err = r.Update(ctx, k8sSA)
		if err != nil {
			errMsg := "Couldn ot annotate a service account"
			r.Recorder.Event(userCR, corev1.EventTypeWarning, "Error", errMsg)
			userCR.Status.Error = errMsg
			log.Error(err, errMsg, "name", k8sSA.GetName())
			return reconcileResultRepeat, err
		}

		userCR.SetFinalizers(helpers.SliceAppendIfMissing(
			userCR.GetFinalizers(),
			consts.FINALIZER_K8S_SERVICE_ACCOUNT,
		))
		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.Status.KafkaUserState.K8sSA = true
		if err := r.updateStatus(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
	}

	if userCR.Status.KafkaUserState.ACLs {
		log.Info("ACLs are configured, checking")
		currentAccess, err := r.listACLs(ctx, userCR.Status.SAEmail)
		if err != nil {
			return reconcileResultNoRepeat, err
		}
		log.Info("Current kafka access", "access", currentAccess)
		slices.SortFunc(currentAccess, func(a, b *kafkawrap.TopicAccess) int {
			return strings.Compare(strings.ToLower(a.Topic), strings.ToLower(b.Topic))
		})

		var desiredAccess []*kafkawrap.TopicAccess

		if len(userCR.Spec.ClusterAccess) > 0 {
			topics, err := r.Opts.KafkaInstance.ListTopics(ctx, true)
			if err != nil {
				return reconcileResultRepeat, err
			}
			log.Info("Got all the topics from the kafka", "amount", len(topics))
			for _, topic := range topics {
				access, err := kafkawrap.NewTopicAccess(topic, userCR.Spec.ClusterAccess)
				if err != nil {
					return reconcileResultRepeat, err
				}
				desiredAccess = append(desiredAccess, access)
			}
		} else {
			desiredAccess, err = castAccessToKafkaFormat(ctx, userCR.Spec.TopicAccess)
			if err != nil {
				return reconcileResultRepeat, err
			}
			log.Info("Desired amount of ACLs", "amount", len(desiredAccess))
		}
		slices.SortFunc(desiredAccess, func(a, b *kafkawrap.TopicAccess) int {
			return strings.Compare(strings.ToLower(a.Topic), strings.ToLower(b.Topic))
		})

		if !reflect.DeepEqual(desiredAccess, currentAccess) {
			userCR.Status.KafkaUserState.ACLs = false
			userCR.Status.Ready = false
			if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
				return reconcileResultRepeat, errUpdate
			}
			return reconcileResultNoRepeat, nil
		}
	} else {
		// Update the ACLs
		log.Info("Updating ACLs")
		if err := r.updateACLs(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
		userCR.SetFinalizers(helpers.SliceAppendIfMissing(
			userCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_ACLS,
		))
		if err := r.updateObject(ctx, userCR); err != nil {
			return reconcileResultNoRepeat, err
		}

		userCR.Status.KafkaUserState.ACLs = true
		if err := r.updateStatus(ctx, userCR); err != nil {
			return reconcileResultRepeat, err
		}
	}
	userCR.Status.Ready = true
	if err := r.updateStatus(ctx, userCR); err != nil {
		return reconcileResultRepeat, err
	}

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

// Is used to setup a watcher that would trigger a reconciliation on a SA change
func (r *KafkaUserReconciler) findKafkaUserForServiceAccount(ctx context.Context, sa client.Object) []reconcile.Request {
	log := logf.FromContext(ctx)
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
			email, ok := sa.GetAnnotations()[consts.ANNOTATION_GKE_EMAIL]
			// If annotation is not set, reconcile the user
			if !ok || email != user.Status.SAEmail {
				user.Status.KafkaUserState.K8sSA = false
				user.Status.Ready = false
				if err := r.updateStatus(ctx, &user); err != nil {
					log.Error(err, "Couldn't update kafka user status")
				}
			}
		}
	}

	return []reconcile.Request{}
}

// Handle cases when resource is deleted
func (r *KafkaUserReconciler) delete(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := logf.FromContext(ctx)
	log.Info("Handle resource deletion")
	// Service accounts have limited name length, we must make sure
	// that we're not exceeding the limit
	gcpServiceAccountName := helpers.StringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	sa, err := getServiceAccount(ctx, r.Opts.GoogleProject, gcpServiceAccountName, 1)
	if err != nil {
		log.Info("Couldn't get a service account, continuing", "error", err)
	} else {
		if err := deleteServiceAccount(ctx, r.Opts.GoogleProject, sa.Email); err != nil {
			log.Error(err, "Couldn't delete a service account")
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.FINALIZER_CLOUD_SERVICE_ACCOUNT,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}
	if err := deleteKafkaIAMBinding(ctx, r.Opts.GoogleProject, userCR.Status.SAEmail); err != nil {
		log.Error(err, "Couldn't add a kafka binding to the project")
		return err
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.FINALIZER_KAFKA_IAM_BINDING,
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
		if k8serrors.IsNotFound(err) {
			log.Info("Service account is not found, continuing...", "sa", userCR.Spec.ServiceAccountName)
		} else {
			return err
		}
	} else {
		delete(k8sSA.Annotations, consts.ANNOTATION_GKE_EMAIL)
		err = r.Update(ctx, k8sSA)
		if err != nil {
			log.Error(err, "Couldn't annotate a service account", "name", k8sSA.GetName())
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.FINALIZER_K8S_SERVICE_ACCOUNT,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	if len(userCR.Spec.ClusterAccess) == 0 {
		if err := r.updateACLs(ctx, userCR); err != nil {
			log.Error(err, "Couldn't update ACLs")
			return err
		}
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.FINALIZER_KAFKA_ACLS,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
		return err
	}

	return nil
}

func castAccessToKafkaFormat(ctx context.Context, input []*gcpkafkav1alpha1.TopicAccess) (result []*kafkawrap.TopicAccess, err error) {
	log := logf.FromContext(ctx)
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

func (r *KafkaUserReconciler) listACLs(ctx context.Context, username string) ([]*kafkawrap.TopicAccess, error) {
	// Get all the topics that are applied to the current user
	log := logf.FromContext(ctx)
	log.Info("Listing the ACLs", "username", username)
	currentAccess, err := r.Opts.KafkaInstance.ListACLs(ctx, username)
	if err != nil {
		log.Error(err, "Couldn't list ACLs")
		return nil, err
	}
	return currentAccess, nil
}

func (r *KafkaUserReconciler) updateACLs(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) (err error) {
	log := logf.FromContext(ctx)
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

func (r *KafkaUserReconciler) getAllowedPermissions(ctx context.Context, name, namespace string) ([]string, error) {
	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: r.Opts.ExtraPermissionsCMNamespace,
		Name:      r.Opts.ExtraPermissionsCM,
	}, configMap); err != nil {
		return nil, err
	}

	data, ok := configMap.Data[fmt.Sprintf("%s_%s", namespace, name)]
	if !ok {
		return []string{}, nil
	}

	return helpers.StringToSlice(data), nil
}

func createServiceAccount(ctx context.Context, projectID, serviceAccountName string) error {
	log := logf.FromContext(ctx)
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
	log := logf.FromContext(ctx)
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
	log := logf.FromContext(ctx)
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
	log := logf.FromContext(ctx)
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
	projectID string,
	roles []string,
	serviceAccountEmail string,
) error {
	log := logf.FromContext(ctx)
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
	for _, role := range roles {
		for _, binding := range updatedPolicy.Bindings {
			// Always add a readWriteRole, becase the real access is managed by ACLs
			if binding.Role == role {
				log.Info("Adding a new member to a role", "member", serviceAccountEmail, "role", role)
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
	log := logf.FromContext(ctx)
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
	log := logf.FromContext(ctx)
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
	log := logf.FromContext(ctx)
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
