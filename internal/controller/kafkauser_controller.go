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
	"time"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/cloud"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	kafkawrap "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
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
	AccountID                   string
	ClientRole                  string
	KafkaInstance               kafkawrap.KafkaImpl
	CloudInstance               cloud.CloudImpl
	AdminUserEmail              string
	ReconcilePeriod             time.Duration
	ExtraPermissionsCMNamespace string
	ExtraPermissionsCM          string
}

var conflictRetry = wait.Backoff{
	Steps:    10,
	Duration: 10 * time.Millisecond,
	Factor:   5.0,
	Jitter:   0.1,
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=kafkausers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=watch;update;list
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=watch;list;get
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//
//nolint:gocyclo
func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	reconcileResultRepeat := reconcile.Result{RequeueAfter: r.Opts.ReconcilePeriod, Requeue: true}
	reconcileResultNoRepeat := reconcile.Result{Requeue: false}

	// Create kafka admin
	admin, err := r.Opts.KafkaInstance.CreateAdmin(ctx)
	if err != nil {
		log.Error(err, "Couldn't create admin")
		return ctrl.Result{}, err
	}
	defer admin.Close()

	// Get the object from the k8s api
	userCR := &gcpkafkav1alpha1.KafkaUser{}
	err = r.Get(ctx, req.NamespacedName, userCR)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcileResultNoRepeat, nil
		}
		log.Error(err, "Could't get a kafka user object")
		return ctrl.Result{}, err
	}

	if userCR.DeletionTimestamp != nil {
		if err := r.delete(ctx, admin, userCR); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Create a service account in the cloud provider
	identityName := helpers.StringSanitize(
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
		_, err = r.Opts.CloudInstance.GetIdentity(ctx, identityName)
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
		sa, err := r.Opts.CloudInstance.CreateIdentity(ctx, identityName)
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

	k8sNS := userCR.GetNamespace()
	k8sSA := userCR.Spec.ServiceAccountName
	identity, err := r.Opts.CloudInstance.GetIdentity(ctx, identityName)
	if err != nil {
		return reconcileResultRepeat, err
	}
	if userCR.Status.KafkaUserState.WorkloadIdentity {
		log.Info("Workload identity was added, checking")
		// If Workload Identity is not applied, set the state to false and return
		if err := r.Opts.CloudInstance.CheckWorkloadIdentity(ctx, k8sNS, k8sSA, identity.Name); err != nil {
			log.Error(err, "Couldn't get workalod identity bindings")
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
		if err := r.Opts.CloudInstance.AddWorkloadIdentity(ctx, k8sNS, k8sSA, identity.Name); err != nil {
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

	allowedPermissions, err := r.getAllowedPermissions(ctx, userCR.GetName(), userCR.GetNamespace())
	if err != nil {
		log.Error(err, "Couldn't get allowed permissions")
		return reconcileResultRepeat, err
	}

	want, err := r.Opts.CloudInstance.BuildDesiredPermissions(ctx, userCR, allowedPermissions)
	if err != nil {
		return reconcileResultRepeat, err
	}

	if userCR.Status.KafkaUserState.IamBindings {
		log.Info("IAM bindings were added, checking", "wanted", want)

		have, err := r.Opts.CloudInstance.GetPermissions(ctx, identity)
		if err != nil {
			return reconcileResultRepeat, err
		}
		if !r.Opts.CloudInstance.EqualPermissions(ctx, want, have) {
			log.Info("Permissions don't match", "applied", have, "desired", want)
			userCR.Status.KafkaUserState.IamBindings = false
			userCR.Status.Ready = false
			if errUpdate := r.updateStatus(ctx, userCR); errUpdate != nil {
				return reconcileResultRepeat, errUpdate
			}
			return reconcileResultNoRepeat, nil
		}
	} else {
		log.Info("Adding IAM bindings", "wanted", want)
		if err := r.Opts.CloudInstance.SetPermissions(ctx, identity, want); err != nil {
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

	k8sServiceAccount := &corev1.ServiceAccount{}
	err = r.Get(ctx, types.NamespacedName{
		Namespace: userCR.GetNamespace(),
		Name:      userCR.Spec.ServiceAccountName,
	}, k8sServiceAccount)
	if err != nil {
		errMsg := "Could not get a k8s service account, make sure it is created"
		r.Recorder.Event(userCR, corev1.EventTypeWarning, "Error", errMsg)
		userCR.Status.Error = errMsg
		log.Error(err, errMsg)
		return reconcileResultRepeat, err
	}

	// If annotation is not set, reconcile the user
	if !r.Opts.CloudInstance.IsSAReady(ctx, userCR, k8sServiceAccount) {
		userCR.Status.KafkaUserState.K8sSA = false
		userCR.Status.Ready = false
	}
	// Update the k8s service account
	if !userCR.Status.KafkaUserState.K8sSA {
		log.Info("Updating the k8s service account")
		annotations := r.Opts.CloudInstance.GetSAAnnotations(ctx, userCR)
		if k8sServiceAccount.Annotations == nil {
			k8sServiceAccount.Annotations = map[string]string{}
		}
		for k, v := range annotations {
			k8sServiceAccount.Annotations[k] = v
		}
		if err := r.Update(ctx, k8sServiceAccount); err != nil {
			errMsg := "Could not ot annotate a service account"
			r.Recorder.Event(userCR, corev1.EventTypeWarning, "Error", errMsg)
			userCR.Status.Error = errMsg
			log.Error(err, errMsg, "name", k8sServiceAccount.GetName())
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
		currentAccess, err := r.listACLs(ctx, admin, userCR.Status.SAEmail)
		if err != nil {
			return reconcileResultNoRepeat, err
		}
		log.Info("Current kafka access", "access", currentAccess)
		slices.SortFunc(currentAccess, func(a, b *kafkawrap.TopicAccess) int {
			return strings.Compare(strings.ToLower(a.Topic), strings.ToLower(b.Topic))
		})

		var desiredAccess []*kafkawrap.TopicAccess

		if len(userCR.Spec.ClusterAccess) > 0 {
			topics, err := r.Opts.KafkaInstance.ListTopics(ctx, admin, true)
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
		if err := r.updateACLs(ctx, admin, userCR); err != nil {
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

	req := []reconcile.Request{}
	kafkaUsers := &gcpkafkav1alpha1.KafkaUserList{}
	if err := r.List(ctx, kafkaUsers, &client.ListOptions{Namespace: namespace}); err != nil {
		log.Error(err, "Couldn't list kafka users")
		return nil
	}

	for _, user := range kafkaUsers.Items {
		if user.Spec.ServiceAccountName == name {
			req = append(req, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      user.Name,
					Namespace: user.Namespace,
				},
			})
			log.Info("ServiceAccount linked to the KafkaUser custom resource issued an event")
		}
	}
	return req
}

// Handle cases when resource is deleted
func (r *KafkaUserReconciler) delete(ctx context.Context, admin *kafka.AdminClient, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := logf.FromContext(ctx)
	log.Info("Handle resource deletion")
	// Service accounts have limited name length, we must make sure
	// that we're not exceeding the limit
	cloudIdentityName := helpers.StringSanitize(
		fmt.Sprintf("%s-%s", userCR.GetNamespace(), userCR.GetName()), 30,
	)

	identity, err := r.Opts.CloudInstance.GetIdentity(ctx, cloudIdentityName)
	if err != nil {
		if errors.Is(err, cloud.ErrNotFound) {
			return nil
		}
		return err
	}

	if err := r.Opts.CloudInstance.DeletePermissions(ctx, identity); err != nil {
		log.Error(err, "Couldn't delete permissions")
		return err
	}

	if err := r.Opts.CloudInstance.DeleteIdentity(ctx, identity); err != nil {
		log.Error(err, "Couldn't delete cloud identity")
		return err
	}

	userCR.SetFinalizers(helpers.SliceRemoveItem(
		userCR.GetFinalizers(),
		consts.FINALIZER_CLOUD_SERVICE_ACCOUNT,
	))
	if err := r.updateObject(ctx, userCR); err != nil {
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
		r.Opts.CloudInstance.CleanupSA(ctx, k8sSA)
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
		if err := r.updateACLs(ctx, admin, userCR); err != nil {
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

func (r *KafkaUserReconciler) listACLs(ctx context.Context, admin *kafka.AdminClient, username string) ([]*kafkawrap.TopicAccess, error) {
	// Get all the topics that are applied to the current user
	log := logf.FromContext(ctx)
	log.Info("Listing the ACLs", "username", username)
	currentAccess, err := r.Opts.KafkaInstance.ListACLs(ctx, admin, username)
	if err != nil {
		log.Error(err, "Couldn't list ACLs")
		return nil, err
	}
	return currentAccess, nil
}

func (r *KafkaUserReconciler) updateACLs(ctx context.Context, admin *kafka.AdminClient, userCR *gcpkafkav1alpha1.KafkaUser) (err error) {
	log := logf.FromContext(ctx)
	var desiredAccess []*kafkawrap.TopicAccess

	if len(userCR.Spec.ClusterAccess) > 0 {
		topics, err := r.Opts.KafkaInstance.ListTopics(ctx, admin, true)
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
		if err := r.Opts.KafkaInstance.CreateACL(ctx, admin, r.Opts.AdminUserEmail, []*kafkawrap.TopicAccess{{
			Topic:     topic.Topic,
			Operation: kafka.ACLOperationAll,
		}}); err != nil {
			return err
		}
	}

	currentAccess, err := r.listACLs(ctx, admin, userCR.Status.SAEmail)
	if err != nil {
		return err
	}

	log.Info("Current amount of ACLs", "amount", len(currentAccess))
	delAccess := findAccessDiff(currentAccess, desiredAccess)
	log.Info("ACLs are marked for removing", "amount", len(delAccess))
	newAccess := findAccessDiff(desiredAccess, currentAccess)
	log.Info("ACLs are marked for creating", "amount", len(newAccess))

	if len(delAccess) > 0 {
		if err := r.Opts.KafkaInstance.DeleteACL(ctx, admin, userCR.Status.SAEmail, delAccess); err != nil {
			log.Error(err, "Couldn't delete ACLs")
			return err
		}
	}
	if len(newAccess) > 0 {
		if err := r.Opts.KafkaInstance.CreateACL(ctx, admin, userCR.Status.SAEmail, newAccess); err != nil {
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

func (r *KafkaUserReconciler) updateStatus(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := logf.FromContext(ctx)
	if err := retry.RetryOnConflict(conflictRetry, func() error {
		if err := r.Status().Update(ctx, userCR); err != nil {
			return err
		}
		if err := r.Get(ctx, client.ObjectKeyFromObject(userCR), userCR); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Error(err, "Failed to update status", "retries", conflictRetry.Steps)
		return err
	}
	return nil
}

func (r *KafkaUserReconciler) updateObject(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) error {
	log := logf.FromContext(ctx)
	if err := retry.RetryOnConflict(conflictRetry, func() error {
		if err := r.Update(ctx, userCR); err != nil {
			return err
		}
		if err := r.Get(ctx, client.ObjectKeyFromObject(userCR), userCR); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Error(err, "Failed to update status", "retries", conflictRetry.Steps)
		return err
	}
	return nil
}
