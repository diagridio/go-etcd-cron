/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioning

import (
	"path/filepath"
	"strconv"
)

// The Organizer decides about key locations.
type Organizer interface {
	JobPath(jobName string) string
	JobsPath(partitionId int) string
	TicksPath(partitionId int) string
}

type organizer struct {
	partitioning Partitioner
	namespace    string
}

func NewOrganizer(namespace string, p Partitioner) Organizer {
	return &organizer{
		partitioning: p,
		namespace:    namespace,
	}
}

func (o *organizer) JobPath(jobName string) string {
	return filepath.Join(o.namespace, "partitions", strconv.Itoa(o.partitioning.CalculatePartitionId(jobName)), "jobs", jobName)
}

func (o *organizer) JobsPath(partitionId int) string {
	return filepath.Join(o.namespace, "partitions", strconv.Itoa(partitionId), "jobs")
}

func (o *organizer) TicksPath(partitionId int) string {
	return filepath.Join(o.namespace, "partitions", strconv.Itoa(partitionId), "ticks")
}
