package consts

const (
	ACCESS_READ_ONLY  = "readOnly"
	ACCESS_READ_WRITE = "readWrite"
)

const (
	FINALIZER_GCP_SERVICE_ACCOUNT     = "k8s.onpier.de/gcp-service-account"
	FINALIZER_CLOUD_SERVICE_ACCOUNT   = "k8s.onpier.de/cloud-service-account"
	FINALIZER_K8S_SERVICE_ACCOUNT     = "k8s.onpier.de/k8s-service-account"
	FINALIZER_CLOUD_WORKLOAD_IDENTITY = "k8s.onpier.de/cloud-workload-identity"
	FINALIZER_KAFKA_IAM_BINDING       = "k8s.onpier.de/kafka-iam-binding"
	FINALIZER_KAFKA_ACLS              = "k8s.onpier.de/kafka-acls"
	FINALIZER_KAFKA_TOPIC             = "k8s.onpier.de/kafka-topic"
)

const (
	ANNOTATION_GKE_EMAIL = "iam.gke.io/gcp-service-account"
)
