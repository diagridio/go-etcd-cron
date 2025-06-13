/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package errors

import (
	"errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	etcderrors "go.etcd.io/etcd/server/v3/etcdserver/errors"

	apierrors "github.com/diagridio/go-etcd-cron/api/errors"
)

// ShouldRetry returns true if the error returned from the handle function
// should be retried.
func ShouldRetry(err error) bool {
	switch {
	case err == nil, apierrors.IsJobAlreadyExists(err):
		return false
	case
		errors.Is(err, etcderrors.ErrTimeout),
		errors.Is(err, etcderrors.ErrTimeoutDueToLeaderFail),
		errors.Is(err, etcderrors.ErrTimeoutDueToConnectionLost),
		errors.Is(err, etcderrors.ErrTimeoutLeaderTransfer),
		errors.Is(err, etcderrors.ErrTimeoutWaitAppliedIndex),
		errors.Is(err, etcderrors.ErrLeaderChanged),
		errors.Is(err, etcderrors.ErrNotEnoughStartedMembers),
		errors.Is(err, etcderrors.ErrTooManyRequests),
		clientv3.IsConnCanceled(err):
		return true
	default:
		return false
	}
}
