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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaUserSpec defines the desired state of User.
type KafkaUserSpec struct {
	ServiceAccountName string         `json:"serviceAccountName"`
	TopicAccess        []*TopicAccess `json:"topicAccess,omitempty"`
	// ReadOnly or ReadWrite access to the whole cluster
	ClusterAccess string `json:"clusterAccess,omitempty"`
	// Permissions that must be additionally assigned to the gcp IAM
	// For example, when an app needs to have access to redis and kafka.
	ExtraPermissions []string `json:"extraPermissions,omitempty"`
}

type TopicAccess struct {
	Topic string `json:"topic"`
	Role  string `json:"role"`
}

type ClusterAccess struct {
	Enabled bool   `json:"enabled"`
	Role    string `json:"role"`
}

// KafkaUserStatus defines the observed state of User.
type KafkaUserStatus struct {
	Ready              bool           `json:"ready"`
	TopicAccessApplied []*TopicAccess `json:"topicAccessApplied,omitempty"`
	SAEmail            string         `json:"saEmail,omitempty"`
	ConfigHash         string         `json:"configHash,omitempty"`
	Error              string         `json:"error,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`,description="Is user ready"
// +kubebuilder:printcolumn:name="Email",type=string,JSONPath=`.status.saEmail`,description="Google SA Email"
// KafkaUser is the Schema for the kafka users API.
type KafkaUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KafkaUserSpec   `json:"spec,omitempty"`
	Status KafkaUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// UserList contains a list of User.
type KafkaUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KafkaUser{}, &KafkaUserList{})
}

func (user *KafkaUser) DoesHaveAccess(topic string) bool {
	for _, access := range user.Status.TopicAccessApplied {
		if access.Topic == topic {
			return true
		}
	}
	for _, access := range user.Spec.TopicAccess {
		if access.Topic == topic {
			return true
		}
	}
	return false
}
