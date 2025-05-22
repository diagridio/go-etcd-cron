/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/internal/engine/queue/actioner"
)

func Test_Fake(t *testing.T) {
	var _ actioner.Interface = New()
}
