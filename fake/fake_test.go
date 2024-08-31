/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package fake

import (
	"testing"

	etcdcron "github.com/diagridio/go-etcd-cron"
)

func Test_Fake(t *testing.T) {
	var _ etcdcron.Interface = new(Fake)
}
