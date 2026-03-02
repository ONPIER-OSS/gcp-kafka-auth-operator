package kafkaconfig

type AWSConfig struct {
	BootstrapServers string
	Region           string
}

type GCPConfig struct {
	BootstrapServers string
}
