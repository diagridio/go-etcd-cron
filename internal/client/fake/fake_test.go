/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake_test

import (
	"testing"

	"github.com/diagridio/go-etcd-cron/internal/client/api"
	"github.com/diagridio/go-etcd-cron/internal/client/fake"
)

func Test_Fake(*testing.T) {
	var _ api.Interface = fake.New()
}
