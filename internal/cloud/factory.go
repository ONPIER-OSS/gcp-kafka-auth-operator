package cloud

import (
	"context"
	"fmt"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
)

func NewCloudInstance(ctx context.Context, env, accountID, clientRole, oidcID, mskARN string) (CloudImpl, error) {
	switch env {
	case consts.EnvGCP:
		return NewGCloudInstance(accountID, clientRole), nil
	case consts.EnvAWS:
		return NewAWSInstance(ctx, oidcID, mskARN)
	default:
		return nil, fmt.Errorf("unsupported env: %s", env)
	}
}
