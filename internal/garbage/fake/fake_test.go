/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/internal/garbage"
)

func Test_Fake(*testing.T) {
	var _ garbage.Interface = New()
}
