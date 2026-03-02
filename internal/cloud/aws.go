package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"net/url"
	"slices"
	"strings"

	gcpkafkav1alpha1 "github.com/ONPIER-playground/gcp-kafka-auth-operator/api/v1alpha1"
	"github.com/ONPIER-playground/gcp-kafka-auth-operator/pkg/consts"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func NewAWSInstance(ctx context.Context, oidcID, mskARN string) (CloudImpl, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	msk, err := parse(mskARN)
	if err != nil {
		return nil, err
	}

	return &AWS{
		Config: cfg,
		OidcID: oidcID,
		MSK:    *msk,
	}, nil
}

func parse(mskArn string) (*MSK, error) {
	parsedARN, err := arn.Parse(mskArn)
	if err != nil {
		return nil, err
	}
	if parsedARN.Service != "kafka" {
		return nil, fmt.Errorf("not a MSK ARN")
	}
	msk := &MSK{
		ARN:       mskArn,
		ParsedARN: parsedARN,
	}
	return msk, err
}

func (a *AWS) CreateIdentity(ctx context.Context, identityName string) (*Identity, error) {
	log := logf.FromContext(ctx)
	log.Info("Creating new role")

	iamClient := iam.NewFromConfig(a.Config)
	// Creating an empty trust policy.
	trustPolicy := TrustPolicyDocument{
		Version: "2012-10-17",
		Statement: []TrustPolicyStatement{{
			Effect:    "Deny",
			Principal: map[string]string{"AWS": "*"},
			Action:    "sts:AssumeRole",
		}},
	}
	policyBytes, err := json.Marshal(trustPolicy)
	if err != nil {
		log.Error(err, "Couldn't create trust policy")
		return nil, err
	}
	result, err := iamClient.CreateRole(ctx, &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(string(policyBytes)),
		RoleName:                 aws.String(identityName),
	})
	if err != nil {
		var exists *iamtypes.EntityAlreadyExistsException
		if errors.As(err, &exists) {
			log.Info("Role already exists, re-using")
			role, err := a.GetIdentity(ctx, identityName)
			if err != nil {
				return nil, err
			}
			return role, nil
		}
		return nil, err
	}
	role := &Identity{
		Name:       aws.ToString(result.Role.RoleName),
		Identifier: aws.ToString(result.Role.Arn),
	}
	return role, err
}

func (a *AWS) GetIdentity(ctx context.Context, identityName string) (*Identity, error) {
	log := logf.FromContext(ctx)
	log.Info("Getting role")

	iamClient := iam.NewFromConfig(a.Config)
	var role *Identity
	result, err := iamClient.GetRole(ctx, &iam.GetRoleInput{
		RoleName: aws.String(identityName),
	})
	if err != nil {
		var notFound *iamtypes.NoSuchEntityException
		if errors.As(err, &notFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	role = &Identity{
		Name:       aws.ToString(result.Role.RoleName),
		Identifier: aws.ToString(result.Role.Arn),
	}
	return role, nil
}

func (a *AWS) DeleteIdentity(ctx context.Context, identity *Identity) error {
	log := logf.FromContext(ctx)
	log.Info("Deleting role")

	iamClient := iam.NewFromConfig(a.Config)
	roleName := identity.Name
	_, err := iamClient.DeleteRole(ctx, &iam.DeleteRoleInput{
		RoleName: aws.String(roleName),
	})
	if err != nil {
		log.Error(err, "Couldn't delete role")
		return err
	}
	return nil
}

func (a *AWS) AddWorkloadIdentity(ctx context.Context, k8sNs, k8sSa, identityName string) error {
	log := logf.FromContext(ctx)
	log.Info("Adding assume role policy")

	iamClient := iam.NewFromConfig(a.Config)
	providerURL := fmt.Sprintf("oidc.eks.%s.amazonaws.com/id/%s", a.MSK.ParsedARN.Region, a.OidcID)
	subject := fmt.Sprintf("system:serviceaccount:%s:%s", k8sNs, k8sSa)
	trustPolicy := TrustPolicyDocument{
		Version: "2012-10-17",
		Statement: []TrustPolicyStatement{{
			Effect: "Allow",
			Principal: map[string]string{
				"Federated": fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", a.MSK.ParsedARN.AccountID, providerURL),
			},
			Action: "sts:AssumeRoleWithWebIdentity",
			Condition: map[string]map[string]string{
				"StringLike": {
					fmt.Sprintf("%s:aud", providerURL): "sts.amazonaws.com",
					fmt.Sprintf("%s:sub", providerURL): subject,
				},
			},
		}},
	}

	policyBytes, err := json.Marshal(trustPolicy)
	if err != nil {
		log.Error(err, "Couldn't create trust policy")
		return err
	}
	_, err = iamClient.UpdateAssumeRolePolicy(ctx, &iam.UpdateAssumeRolePolicyInput{
		RoleName:       aws.String(identityName),
		PolicyDocument: aws.String(string(policyBytes)),
	})
	if err != nil {
		log.Error(err, "Couldn't update assume role policy")
		return err
	}

	return nil
}

func (a *AWS) CheckWorkloadIdentity(ctx context.Context, k8sNs, k8sSa, identityName string) error {
	log := logf.FromContext(ctx)
	log.Info("Checking assume role policy")

	iamClient := iam.NewFromConfig(a.Config)
	result, err := iamClient.GetRole(ctx, &iam.GetRoleInput{
		RoleName: aws.String(identityName),
	})
	if err != nil {
		log.Error(err, "Couldn't get role")
		return err
	}

	rawPolicy, err := url.QueryUnescape(aws.ToString(result.Role.AssumeRolePolicyDocument))
	if err != nil {
		log.Error(err, "Couldn't decode policy document")
		return err
	}

	var policy TrustPolicyDocument
	if err := json.Unmarshal([]byte(rawPolicy), &policy); err != nil {
		log.Error(err, "Couldn't unmarshal policy document")
		return err
	}

	providerURL := fmt.Sprintf("oidc.eks.%s.amazonaws.com/id/%s", a.MSK.ParsedARN.Region, a.OidcID)
	expectedFederated := fmt.Sprintf("arn:aws:iam::%s:oidc-provider/%s", a.MSK.ParsedARN.AccountID, providerURL)
	expectedAud := "sts.amazonaws.com"
	expectedSub := fmt.Sprintf("system:serviceaccount:%s:%s", k8sNs, k8sSa)
	expectedAudKey := fmt.Sprintf("%s:aud", providerURL)
	expectedSubKey := fmt.Sprintf("%s:sub", providerURL)

	for _, stmt := range policy.Statement {
		if stmt.Effect != "Allow" {
			continue
		}
		if stmt.Principal["Federated"] != expectedFederated {
			continue
		}
		conds, ok := stmt.Condition["StringLike"]
		if !ok {
			continue
		}
		if conds[expectedAudKey] != expectedAud {
			continue
		}
		if conds[expectedSubKey] != expectedSub {
			continue
		}
		return nil
	}
	return fmt.Errorf("assume role policy doesn't have enough permissions")
}

func (a *AWS) GetPermissions(ctx context.Context, identity *Identity) (*DesiredPermissions, error) {
	log := logf.FromContext(ctx)
	log.Info("Getting inline policies")

	iamClient := iam.NewFromConfig(a.Config)
	policyName := "kafka-user-policy"
	out, err := iamClient.GetRolePolicy(ctx, &iam.GetRolePolicyInput{
		RoleName:   aws.String(identity.Name),
		PolicyName: aws.String(policyName),
	})
	if err != nil {
		log.Error(err, "Couldn't get role policy")
		return nil, err
	}

	decoded, err := url.QueryUnescape(aws.ToString(out.PolicyDocument))
	if err != nil {
		log.Error(err, "Couldn't decode policy document")
		return nil, err
	}

	return &DesiredPermissions{
		InlinePolicies: map[string]string{
			policyName: decoded,
		},
	}, nil
}

func (a *AWS) SetPermissions(ctx context.Context, identity *Identity, permissions *DesiredPermissions) error {
	log := logf.FromContext(ctx)
	log.Info("Creating inline policies")

	iamClient := iam.NewFromConfig(a.Config)
	// Create the inline policy
	for policyName, policyDoc := range permissions.InlinePolicies {
		_, err := iamClient.PutRolePolicy(ctx, &iam.PutRolePolicyInput{
			RoleName:       aws.String(identity.Name),
			PolicyName:     aws.String(policyName),
			PolicyDocument: aws.String(policyDoc),
		})
		if err != nil {
			log.Error(err, "Couldn't attach inline policy")
		}
	}
	// TO-DO:Delete existing policies if they do not match policyName
	return nil
}

func (a *AWS) EqualPermissions(ctx context.Context, want, have *DesiredPermissions) bool {
	log := logf.FromContext(ctx)
	log.Info("Checking if the desired permissions are applied")

	if want == nil && have == nil {
		return true
	}
	if want == nil || have == nil {
		return false
	}

	return cmp.Equal(
		want,
		have,
		cmpopts.SortSlices(func(a, b string) bool { return a < b }),
		cmpopts.SortSlices(func(a, b InlinePolicyStatement) bool {
			return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
		}),
	)
}

func (a *AWS) DeletePermissions(ctx context.Context, identity *Identity) error {
	log := logf.FromContext(ctx)
	log.Info("Deleting inline policies")

	iamClient := iam.NewFromConfig(a.Config)
	identityName := identity.Name
	// delete inline policies
	currentInline, err := a.GetPermissions(ctx, identity)
	if err != nil {
		log.Error(err, "Couldn't get role permissions")
		return err
	}
	for have := range currentInline.InlinePolicies {
		_, err := iamClient.DeleteRolePolicy(ctx, &iam.DeleteRolePolicyInput{
			RoleName:   aws.String(identityName),
			PolicyName: aws.String(have),
		})
		if err != nil {
			log.Error(err, "Couldn't delete inline policies")
			return err
		}
	}

	return nil
}

func (a *AWS) GetSAAnnotations(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser) map[string]string {
	log := logf.FromContext(ctx)
	log.Info("Getting service account annotations")

	return map[string]string{
		"eks.amazonaws.com/role-arn": userCR.Status.SAEmail,
	}
}

func (a *AWS) IsSAReady(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, sa *corev1.ServiceAccount) bool {
	log := logf.FromContext(ctx)
	log.Info("checking if service account is ready")

	arn, ok := sa.GetAnnotations()[consts.ANNOTATION_AWS_IRSA]
	return ok && arn == userCR.Status.SAEmail
}

func (a *AWS) CleanupSA(ctx context.Context, sa *corev1.ServiceAccount) {
	log := logf.FromContext(ctx)
	log.Info("Deleting service account annotation")

	if sa.Annotations == nil {
		return
	}
	delete(sa.Annotations, consts.ANNOTATION_AWS_IRSA)
}

func (a *AWS) BuildDesiredPermissions(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, allowedPermissions []string) (*DesiredPermissions, error) {
	log := logf.FromContext(ctx)
	log.Info("Building desired permissions")

	//nolint:prealloc
	statements := []InlinePolicyStatement{
		{
			Effect: "Allow",
			Action: []string{
				"kafka-cluster:Connect",
				"kafka-cluster:DescribeCluster",
			},
			Resource: []string{a.MSK.ARN},
		},
	}

	for _, ta := range userCR.Spec.TopicAccess {

		topicArn := strings.Replace(a.MSK.ARN, ":cluster/", ":topic/", 1) + "/" + ta.Topic
		actions := []string{
			"kafka-cluster:DescribeTopic",
			"kafka-cluster:ReadData",
		}
		if ta.Role == "readWrite" {
			actions = append(actions, "kafka-cluster:WriteData")
		}
		statements = append(statements, InlinePolicyStatement{
			Effect:   "Allow",
			Action:   actions,
			Resource: []string{topicArn},
		})
	}
	groupArn := strings.Replace(a.MSK.ARN, ":cluster/", ":group/", 1) + "/*"
	statements = append(statements, InlinePolicyStatement{
		Effect: "Allow",
		Action: []string{
			"kafka-cluster:AlterGroup",
			"kafka-cluster:DescribeGroup",
		},
		Resource: []string{groupArn},
	})
	policy := InlinePolicyDocument{
		Version:   "2012-10-17",
		Statement: statements,
	}

	if len(userCR.Spec.ExtraRoles) > 0 {
		extraStatements, err := a.buildExtraPermissions(ctx, userCR, allowedPermissions)
		if err != nil {
			log.Error(err, "Couldn't build extra permissions")
			return nil, err
		}
		policy.Statement = append(policy.Statement, extraStatements...)
	}

	b, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}

	return &DesiredPermissions{
		InlinePolicies: map[string]string{
			"kafka-user-policy": string(b),
		},
	}, nil
}

func (a *AWS) buildExtraPermissions(ctx context.Context, userCR *gcpkafkav1alpha1.KafkaUser, allowedPermissions []string) ([]InlinePolicyStatement, error) {
	log := logf.FromContext(ctx)
	log.Info("Building extra permissions")

	var statements []InlinePolicyStatement

	for _, extraRole := range userCR.Spec.ExtraRoles {
		if !slices.Contains(allowedPermissions, extraRole.Type) {
			err := errors.New("extra permission is not allowed")
			errMsg := fmt.Sprintf("%s permissions needs to be added to allowed permissions", extraRole.Type)
			log.Error(err, errMsg)
			continue
		}
		// Ignore GCP-style entries
		if extraRole.Type == "" {
			err := errors.New("empty extraRoles.type is not allowed")
			errMsg := fmt.Sprintf("Type must be one of %s", allowedPermissions)
			log.Error(err, errMsg)
		}

		switch extraRole.Type {
		case "s3":
			stmts, err := a.buildS3Statements(ctx, extraRole)
			if err != nil {
				log.Error(err, "Couldn't build s3 permissions")
				return nil, err
			}
			statements = append(statements, stmts...)
		// TO-DO: Add the other typs like redis etc
		default:
			return nil, fmt.Errorf("unsupported extraRole type: %s", extraRole.Type)
		}
	}
	return statements, nil
}

func (a *AWS) buildS3Statements(ctx context.Context, access gcpkafkav1alpha1.ExtraRole) ([]InlinePolicyStatement, error) {
	log := logf.FromContext(ctx)
	log.Info("Getting IAM bindings")

	if access.Bucket == "" {
		return nil, fmt.Errorf("s3 access requires bucket name")
	}

	bucketArn := fmt.Sprintf("arn:aws:s3:::%s", access.Bucket)
	objectArn := fmt.Sprintf("arn:aws:s3:::%s/*", access.Bucket)

	var actions []string

	switch access.Permission {

	case "readOnly":
		actions = []string{
			"s3:GetObject",
			"s3:ListBucket",
		}

	case "readWrite":
		actions = []string{
			"s3:GetObject",
			"s3:ListBucket",
			"s3:PutObject",
			"s3:AbortMultipartUpload",
			"s3:DeleteObject",
		}
	default:
		return nil, fmt.Errorf("invalid s3 permission: %s", access.Permission)
	}

	return []InlinePolicyStatement{
		{
			Effect:   "Allow",
			Action:   actions,
			Resource: []string{bucketArn, objectArn},
		},
	}, nil
}
