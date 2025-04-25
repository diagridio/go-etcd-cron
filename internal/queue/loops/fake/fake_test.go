/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/internal/queue/loops"
)

func Test_Fake(t *testing.T) {
	var _ loops.Interface[int] = New[int]()
}
