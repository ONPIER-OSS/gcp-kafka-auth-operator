package cloud

import (
	"context"
	"fmt"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
)

func TestCheckCleanupPolicies(t *testing.T) {
	fmt.Println("TESTINGGGGGGG")
	saEmail := "test@test.test"
	policy := &iampb.Policy{
		Version: 0,
		Bindings: []*iampb.Binding{
			{
				Role:    "test1",
				Members: []string{"check@check.check", "test@test.test"},
			},
			{
				Role:    "test2",
				Members: []string{"test@test.test"},
			},
			{
				Role:    "test3",
				Members: []string{"check@check.check"},
			},
		},
	}

	newPolicy := cleanUpPolicy(context.TODO(), saEmail, policy)
	assert.Equal(t, []string{"check@check.check"}, newPolicy.Bindings[0].Members)
	assert.Equal(t, []string(nil), newPolicy.Bindings[1].Members)
	assert.Equal(t, []string{"check@check.check"}, newPolicy.Bindings[2].Members)
}
