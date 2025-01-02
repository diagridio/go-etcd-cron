/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package key

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_JobKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace string
		id        string
		jobName   string
		expJobKey string
	}{
		{
			namespace: "",
			jobName:   "abc",
			id:        "0",
			expJobKey: "jobs/abc",
		},
		{
			namespace: "123",
			jobName:   "abc",
			id:        "0",
			expJobKey: "123/jobs/abc",
		},
		{
			namespace: "/123",
			jobName:   "def",
			id:        "1",
			expJobKey: "/123/jobs/def",
		},
	}

	for _, test := range tests {
		t.Run(test.namespace+"/"+test.jobName, func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: test.namespace,
				ID:        test.id,
			})
			require.NoError(t, err)
			assert.Equal(t, test.expJobKey, key.JobKey(test.jobName))
		})
	}
}

func Test_CounterKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace     string
		id            string
		jobName       string
		expCounterKey string
	}{
		{
			namespace:     "",
			jobName:       "abc",
			id:            "0",
			expCounterKey: "counters/abc",
		},
		{
			namespace:     "123",
			jobName:       "abc",
			id:            "0",
			expCounterKey: "123/counters/abc",
		},
		{
			namespace:     "/123",
			jobName:       "def",
			id:            "1",
			expCounterKey: "/123/counters/def",
		},
	}

	for _, test := range tests {
		t.Run(test.namespace+"/"+test.jobName, func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: test.namespace,
				ID:        test.id,
			})
			require.NoError(t, err)
			assert.Equal(t, test.expCounterKey, key.CounterKey(test.jobName))
		})
	}
}

func Test_LeadershipNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace       string
		expLeadershipNS string
	}{
		{
			namespace:       "",
			expLeadershipNS: "leadership",
		},
		{
			namespace:       "123",
			expLeadershipNS: "123/leadership",
		},
		{
			namespace:       "/123/abc",
			expLeadershipNS: "/123/abc/leadership",
		},
	}

	for _, test := range tests {
		t.Run(test.namespace, func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: test.namespace,
				ID:        "123",
			})
			require.NoError(t, err)
			assert.Equal(t, test.expLeadershipNS, key.LeadershipNamespace())
		})
	}
}

func Test_LeadershipKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		namespace        string
		id               string
		expLeadershipKey string
	}{
		{
			namespace:        "",
			id:               "0",
			expLeadershipKey: "leadership/0",
		},
		{
			namespace:        "123",
			id:               "0",
			expLeadershipKey: "123/leadership/0",
		},
		{
			namespace:        "/123/abc",
			id:               "3",
			expLeadershipKey: "/123/abc/leadership/3",
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s/%s", test.namespace, test.id), func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: test.namespace,
				ID:        test.id,
			})
			require.NoError(t, err)
			assert.Equal(t, test.expLeadershipKey, key.LeadershipKey())
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
		t.Run(test.namespace, func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: test.namespace,
				ID:        "123",
			})
			require.NoError(t, err)
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
		t.Run(test.key, func(t *testing.T) {
			t.Parallel()
			key, err := New(Options{
				Namespace: "/123",
				ID:        "123",
			})
			require.NoError(t, err)
			assert.Equal(t, test.expJobName, key.JobName([]byte(test.key)))
		})
	}
}
