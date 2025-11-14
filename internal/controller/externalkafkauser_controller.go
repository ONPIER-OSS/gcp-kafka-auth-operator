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
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	kafkawrap "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
)

// ExternalKafkaUserReconciler reconciles a ExternalKafkaUser object
type ExternalKafkaUserReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Opts     *ExternalKafkaUserOpts
	Recorder record.EventRecorder
}

type ExternalKafkaUserOpts struct {
	KafkaInstance   kafkawrap.KafkaImpl
	AdminUserEmail  string
	ReconcilePeriod time.Duration
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=externalkafkausers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=externalkafkausers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=externalkafkausers/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

func (r *ExternalKafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	reconcileResultRepeat := reconcile.Result{RequeueAfter: r.Opts.ReconcilePeriod, Requeue: true}
	reconcileResultNoRepeat := reconcile.Result{Requeue: false}

	externalUserCR := &gcpkafkav1alpha1.ExternalKafkaUser{}
	if err := r.Get(ctx, req.NamespacedName, externalUserCR); err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcileResultNoRepeat, nil
		}
		log.Error(err, "Couldn't get an external user object")
		return reconcileResultRepeat, err
	}

	if externalUserCR.DeletionTimestamp != nil {
		if err := r.updateACLs(ctx, externalUserCR); err != nil {
			errMsg := "Error while updating ACLs"
			r.Recorder.Event(externalUserCR, corev1.EventTypeWarning, "Error", errMsg)
			externalUserCR.Status.Error = errMsg
			if errUpdate := r.updateStatus(ctx, externalUserCR); errUpdate != nil {
				log.Error(err, "Couldn't update status on error")
				// Return the real error here to make it visible
				return reconcileResultRepeat, err
			}
			log.Error(err, errMsg)
			return reconcileResultRepeat, err
		}
		externalUserCR.SetFinalizers(helpers.SliceRemoveItem(
			externalUserCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_ACLS,
		))
		if err := r.updateObject(ctx, externalUserCR); err != nil {
			return reconcileResultRepeat, err
		}
		return ctrl.Result{}, nil
	}

	hash, err := helpers.GetHashFromAnything(externalUserCR.Spec.DeepCopy())
	if err != nil {
		return reconcileResultRepeat, nil
	}

	if externalUserCR.Status.ConfigHash != hash {
		log.Info("Spec was changed", "oldHash", externalUserCR.Status.ConfigHash, "newHash", hash)
		externalUserCR.Status.ConfigHash = hash
		externalUserCR.Status.Ready = false
		if err := r.updateStatus(ctx, externalUserCR); err != nil {
			log.Error(err, "Couldn't update an object on defer")
			return reconcileResultRepeat, err
		}
		// Returning here, because the status change will trigger a new
		// reconcliation and will sync the state
		return reconcileResultNoRepeat, nil
	}

	if !externalUserCR.Status.Ready {
		log.Info("Reconciling the external user object")
		defer func() {
			if err := r.updateStatus(ctx, externalUserCR); err != nil {
				log.Error(err, "Couldn't update an object on defer")
			}
		}()

		if err := r.updateACLs(ctx, externalUserCR); err != nil {
			errMsg := "Could not update ACLs"
			r.Recorder.Event(externalUserCR, corev1.EventTypeWarning, "Error", errMsg)
			externalUserCR.Status.Error = errMsg
			log.Error(err, errMsg)
			return reconcileResultRepeat, err
		}
		externalUserCR.SetFinalizers(helpers.SliceAppendIfMissing(
			externalUserCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_ACLS,
		))
		if err := r.updateObject(ctx, externalUserCR); err != nil {
			return reconcileResultRepeat, err
		}

	}

	return ctrl.Result{}, nil
}

func (r *ExternalKafkaUserReconciler) updateACLs(ctx context.Context, externalUserCR *gcpkafkav1alpha1.ExternalKafkaUser) (err error) {
	log := logf.FromContext(ctx)
	var desiredAccess []*kafkawrap.TopicAccess
	if externalUserCR.DeletionTimestamp == nil {
		desiredAccess, err = castAccessToKafkaFormat(ctx, externalUserCR.Spec.TopicAccess)
		if err != nil {
			return err
		}
	}

	log.Info("Desired amount of ACLs", "amount", len(desiredAccess))

	// Append the operator user to every topic, so it doesn't lose access
	for _, topic := range desiredAccess {
		if err := r.Opts.KafkaInstance.CreateACL(ctx, r.Opts.AdminUserEmail, []*kafkawrap.TopicAccess{{
			Topic:     topic.Topic,
			Operation: kafka.ACLOperationAll,
		}}); err != nil {
			return err
		}
	}

	currentAccess, err := castAccessToKafkaFormat(ctx, externalUserCR.Status.TopicAccessApplied)
	if err != nil {
		return err
	}

	log.Info("Current amount of ACLs", "amount", len(currentAccess))
	delAccess := findAccessDiff(currentAccess, desiredAccess)
	log.Info("ACLs are marked for removing", "amount", len(delAccess))
	newAccess := findAccessDiff(desiredAccess, currentAccess)
	log.Info("ACLs are marked for creating", "amount", len(newAccess))

	if len(delAccess) > 0 {
		if err := r.Opts.KafkaInstance.DeleteACL(ctx, *externalUserCR.Spec.Username, delAccess); err != nil {
			log.Error(err, "Couldn't delete ACLs")
			return err
		}
	}
	if len(newAccess) > 0 {
		if err := r.Opts.KafkaInstance.CreateACL(ctx, *externalUserCR.Spec.Username, newAccess); err != nil {
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
	externalUserCR.Status.TopicAccessApplied = appliedTopics
	externalUserCR.Status.Ready = true
	if err := r.updateStatus(ctx, externalUserCR); err != nil {
		return err
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ExternalKafkaUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gcpkafkav1alpha1.ExternalKafkaUser{}).
		Named("externalkafkauser").
		Complete(r)
}

func (r *ExternalKafkaUserReconciler) updateStatus(ctx context.Context, externalUserCR *gcpkafkav1alpha1.ExternalKafkaUser) error {
	log := logf.FromContext(ctx)
	if err := r.Status().Update(ctx, externalUserCR); err != nil {
		log.Error(err, "failed to update status")
		return err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(externalUserCR), externalUserCR); err != nil {
		log.Error(err, "Failed to get an updated object")
		return err
	}
	return nil
}

func (r *ExternalKafkaUserReconciler) updateObject(ctx context.Context, externalUserCR *gcpkafkav1alpha1.ExternalKafkaUser) error {
	log := logf.FromContext(ctx)
	if err := r.Update(ctx, externalUserCR); err != nil {
		log.Error(err, "failed to update status")
		return err
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(externalUserCR), externalUserCR); err != nil {
		log.Error(err, "Failed to get an updated object")
		return err
	}
	return nil
}
