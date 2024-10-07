/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/internal/counter"
)

func Test_Fake(*testing.T) {
	var _ counter.Interface = New()
}
