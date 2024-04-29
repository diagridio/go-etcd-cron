/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package key

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_JobKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace   string
		partitionID uint32
		jobName     string
		expJobKey   string
	}{
		{
			namespace:   "",
			jobName:     "abc",
			partitionID: 0,
			expJobKey:   "jobs/abc",
		},
		{
			namespace:   "123",
			jobName:     "abc",
			partitionID: 0,
			expJobKey:   "123/jobs/abc",
		},
		{
			namespace:   "/123",
			jobName:     "def",
			partitionID: 1,
			expJobKey:   "/123/jobs/def",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.namespace+"/"+test.jobName, func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   test.namespace,
				PartitionID: test.partitionID,
			})
			assert.Equal(t, test.expJobKey, key.JobKey(test.jobName))
		})
	}
}

func Test_CounterKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace     string
		partitionID   uint32
		jobName       string
		expCounterKey string
	}{
		{
			namespace:     "",
			jobName:       "abc",
			partitionID:   0,
			expCounterKey: "counters/abc",
		},
		{
			namespace:     "123",
			jobName:       "abc",
			partitionID:   0,
			expCounterKey: "123/counters/abc",
		},
		{
			namespace:     "/123",
			jobName:       "def",
			partitionID:   1,
			expCounterKey: "/123/counters/def",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.namespace+"/"+test.jobName, func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   test.namespace,
				PartitionID: test.partitionID,
			})
			assert.Equal(t, test.expCounterKey, key.CounterKey(test.jobName))
		})
	}
}

func Test_LeaseNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace  string
		expLeaseNS string
	}{
		{
			namespace:  "",
			expLeaseNS: "leases",
		},
		{
			namespace:  "123",
			expLeaseNS: "123/leases",
		},
		{
			namespace:  "/123/abc",
			expLeaseNS: "/123/abc/leases",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.namespace, func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   test.namespace,
				PartitionID: 123,
			})
			assert.Equal(t, test.expLeaseNS, key.LeaseNamespace())
		})
	}
}

func Test_LeaseKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace   string
		partitionID uint32
		expLeaseKey string
	}{
		{
			namespace:   "",
			partitionID: 0,
			expLeaseKey: "leases/0",
		},
		{
			namespace:   "123",
			partitionID: 0,
			expLeaseKey: "123/leases/0",
		},
		{
			namespace:   "/123/abc",
			partitionID: 3,
			expLeaseKey: "/123/abc/leases/3",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%s/%d", test.namespace, test.partitionID), func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   test.namespace,
				PartitionID: test.partitionID,
			})
			assert.Equal(t, test.expLeaseKey, key.LeaseKey())
		})
	}
}

func Test_JobNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace string
		expJobNS  string
	}{
		{
			namespace: "",
			expJobNS:  "jobs",
		},
		{
			namespace: "123",
			expJobNS:  "123/jobs",
		},
		{
			namespace: "/123/abc",
			expJobNS:  "/123/abc/jobs",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.namespace, func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   test.namespace,
				PartitionID: 123,
			})
			assert.Equal(t, test.expJobNS, key.JobNamespace())
		})
	}
}

func Test_JobName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key        string
		expJobName string
	}{
		{
			key:        "jobs/abc",
			expJobName: "abc",
		},
		{
			key:        "123/jobs/abc",
			expJobName: "abc",
		},
		{
			key:        "/123/abc/jobs/def",
			expJobName: "def",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			key := New(Options{
				Namespace:   "/123",
				PartitionID: 123,
			})
			assert.Equal(t, test.expJobName, key.JobName([]byte(test.key)))
		})
	}
}
