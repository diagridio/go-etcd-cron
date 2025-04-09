/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package errors

import "fmt"

// JobAlreadyExists is an error type that indicates a Job already exists in the
// store.
type JobAlreadyExists struct {
	err string
}

func (j JobAlreadyExists) Error() string {
	return j.err
}

func NewJobAlreadyExists(job string) JobAlreadyExists {
	return JobAlreadyExists{err: fmt.Sprintf("job already exists: '%s'", job)}
}

func IsJobAlreadyExists(err error) bool {
	_, ok := err.(JobAlreadyExists)
	return ok
}
