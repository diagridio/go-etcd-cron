/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package errors

import (
	"errors"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"

	apierrors "github.com/diagridio/go-etcd-cron/api/errors"
)

// ShouldRetry returns true if the error returned from the handle function
// should be retried.
func ShouldRetry(err error) bool {
	switch {
	case err == nil, apierrors.IsJobAlreadyExists(err):
		return false
	case
		errors.Is(err, rpctypes.ErrTimeout),
		errors.Is(err, rpctypes.ErrTimeoutDueToLeaderFail),
		errors.Is(err, rpctypes.ErrTimeoutDueToConnectionLost),
		errors.Is(err, rpctypes.ErrTimeoutWaitAppliedIndex),
		errors.Is(err, rpctypes.ErrLeaderChanged),
		errors.Is(err, rpctypes.ErrTooManyRequests),
		errors.Is(err, etcdserver.ErrTimeout),
		errors.Is(err, etcdserver.ErrTimeoutDueToLeaderFail),
		errors.Is(err, etcdserver.ErrTimeoutDueToConnectionLost),
		errors.Is(err, etcdserver.ErrTimeoutLeaderTransfer),
		errors.Is(err, etcdserver.ErrTimeoutWaitAppliedIndex),
		errors.Is(err, etcdserver.ErrLeaderChanged),
		errors.Is(err, etcdserver.ErrNotEnoughStartedMembers),
		errors.Is(err, etcdserver.ErrTooManyRequests),
		clientv3.IsConnCanceled(err):
		return true
	default:
		return false
	}
}
