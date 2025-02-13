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

// ClusterTopicSpec defines the desired state of ClusterTopic.
type ClusterTopicSpec struct {
	DeletionProtected bool              `json:"deletionProtected,omitempty"`
	NumPartitions     int               `json:"numPartitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Config            map[string]string `json:"config,omitempty"`
}

// ClusterTopicStatus defines the observed state of ClusterTopic.
type ClusterTopicStatus struct {
	Ready bool `json:"ready"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=topic
// +kubebuilder:printcolumn:name="DeletionProtected",type=string,JSONPath=`.spec.deletionProtected`,description="Is topic deletion protected"
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.ready`,description="Is topic ready"
// +kubebuilder:subresource:status

// ClusterTopic is the Schema for the clustertopics API.
type ClusterTopic struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterTopicSpec   `json:"spec,omitempty"`
	Status ClusterTopicStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterTopicList contains a list of ClusterTopic.
type ClusterTopicList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterTopic `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterTopic{}, &ClusterTopicList{})
}
