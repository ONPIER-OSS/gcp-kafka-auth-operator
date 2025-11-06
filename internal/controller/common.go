package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Handle the reconciliation error
func reconcileError(ctx context.Context,
	reason string,
	errMsg string,
	obj client.Object,
	rec record.EventRecorder,
) {
	rec.Event(obj, corev1.EventTypeWarning, reason, errMsg)
}
