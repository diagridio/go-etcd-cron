/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake_test

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/api"
	"github.com/diagridio/go-etcd-cron/tests/framework/fake"
)

func Test_Fake(t *testing.T) {
	t.Parallel()
	var _ api.Interface = new(fake.Fake)
}
