package consts

const (
	READ_ONLY_ACCESS  = "readOnly"
	READ_WRITE_ACCESS = "readWrite"
)

const (
	GCP_SERVICE_ACCOUNT_FINALIZER   = "k8s.onpier.de/gcp-service-account"
	K8S_SERVICE_ACCOUNT_FINALIZER   = "k8s.onpier.de/k8s-service-account"
	GCP_WORKLOAD_IDENTITY_FINALIZER = "k8s.onpier.de/gcp-workload-identity"
	KAFKA_IAM_BINDING_FINALIZER     = "k8s.onpier.de/kafka-iam-binding"
	KAFKA_ACLS_FINALIZER            = "k8s.onpier.de/kafka-acls"
	KAFKA_TOPIC_FINALZER            = "k8s.onpier.de/kafka-topic"
)
