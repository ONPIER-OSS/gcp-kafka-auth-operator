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

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/helpers"
	kafkawrap "github.com/ONPIER-playground/gcp-kafka-auth-operator/internal/kafka"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ClusterTopicReconciler reconciles a ClusterTopic object
type ClusterTopicReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	KafkaInstance  kafkawrap.KafkaImpl
	AdminUserEmail string
}

// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=clustertopics,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=clustertopics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gcp-kafka.k8s.onpier.de,resources=clustertopics/finalizers,verbs=update

func (r *ClusterTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	// TODO: Should be configurable
	log.Info("Topic creation is not production ready yet", "request", req.String())
	reconcilePeriod := time.Minute
	reconcileResult := reconcile.Result{RequeueAfter: reconcilePeriod}

	topicCR := &gcpkafkav1alpha1.ClusterTopic{}
	err := r.Get(ctx, req.NamespacedName, topicCR)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return reconcileResult, err
	}

	if topicCR.DeletionTimestamp != nil {
		if err := r.KafkaInstance.RemoveTopic(ctx, topicCR.GetName()); err != nil {
			log.Error(err, "Reconciliation failed")
			return reconcileResult, err
		}
		topicCR.SetFinalizers(helpers.SliceRemoveItem(
			topicCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_TOPIC,
		))
		if err := r.Update(ctx, topicCR); err != nil {
			return reconcileResult, err
		}
		if err := r.KafkaInstance.DeleteACL(ctx, r.AdminUserEmail, []*kafkawrap.TopicAccess{{
			Topic:     topicCR.Name,
			Operation: kafka.ACLOperationAll,
		}}); err != nil {
			return reconcileResult, err
		}

		return ctrl.Result{}, nil
	}

	// Update object status always when function exit abnormally or through a panic.
	defer func() {
		if err := r.Status().Update(ctx, topicCR); err != nil {
			log.Error(err, "failed to update status")
		}
	}()
	defer func() {
		if err := r.updateUserCRs(ctx, topicCR.Name); err != nil {
			log.Error(err, "Couldn't notify users about changes")
		}
	}()
	if err := r.KafkaInstance.CreateTopic(
		ctx,
		topicCR.GetName(),
		topicCR.Spec.NumPartitions,
		topicCR.Spec.ReplicationFactor,
		topicCR.Spec.Config,
	); err != nil {
		log.Error(err, "Reconciliation failed")
		return reconcileResult, err
	}
	if !topicCR.Spec.DeletionProtected {
		topicCR.SetFinalizers(helpers.SliceAppendIfMissing(
			topicCR.GetFinalizers(),
			consts.FINALIZER_KAFKA_TOPIC,
		))
		if err := r.Update(ctx, topicCR); err != nil {
			return reconcileResult, err
		}
		if err := r.Get(ctx, req.NamespacedName, topicCR); err != nil {
			return reconcileResult, err
		}
		if err := r.KafkaInstance.CreateACL(ctx, r.AdminUserEmail, []*kafkawrap.TopicAccess{{
			Topic:     topicCR.Name,
			Operation: kafka.ACLOperationAll,
		}}); err != nil {
			return reconcileResult, err
		}
	}

	topicCR.Status.Ready = true

	return ctrl.Result{}, nil
}

func (r *ClusterTopicReconciler) updateUserCRs(ctx context.Context, topicName string) error {
	kafkaUsers := &gcpkafkav1alpha1.KafkaUserList{}
	if err := r.List(ctx, kafkaUsers); err != nil {
		return err
	}
	for _, user := range kafkaUsers.Items {
		if len(user.Spec.ClusterAccess) > 0 {
			user.Status.Ready = false
			user.Status.KafkaUserState.ACLs = false
			if err := r.Status().Update(ctx, &user); err != nil {
				return err
			}
		} else {
			if user.DoesHaveAccess(topicName) {
				user.Status.Ready = false
				user.Status.KafkaUserState.ACLs = false
				if err := r.Status().Update(ctx, &user); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&gcpkafkav1alpha1.ClusterTopic{}).
		Named("clustertopic").
		Complete(r)
}
