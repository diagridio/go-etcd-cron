/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package etcdcron

import (
	"fmt"
	"hash/fnv"
)

type Partitioning struct {
	// Number of hosts to compete to leadership of partitions.
	numHosts int
	// Maximum number of virtual partitions to split the jobs.
	numVirtualPartitions int
	// This hostId (must be between 0 and numHosts)
	hostId int
}

// NoPartitioning will make any host a leader of any partition.
func NoPartitioning() *Partitioning {
	p, _ := NewPartitioning(1, 1, 0)
	return p
}

// NewPartitioning holds the logic to hold job partitioning
func NewPartitioning(numVirtualPartitions int, numHosts int, hostId int) (*Partitioning, error) {
	if numVirtualPartitions < numHosts {
		return nil, fmt.Errorf("cannot have more hosts than partitions")
	}
	if hostId < 0 {
		return nil, fmt.Errorf("hostId cannot be zero")
	}
	if hostId >= numHosts {
		return nil, fmt.Errorf("hostId cannot be greater or equal to %d", numHosts)
	}
	return &Partitioning{
		numVirtualPartitions: numVirtualPartitions,
		numHosts:             numHosts,
		hostId:               hostId,
	}, nil
}

func (p *Partitioning) CalculatePartitionId(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(p.numVirtualPartitions))
}

func (p *Partitioning) CheckPartitionLeader(partitionId int) bool {
	// First, make sure partitionId is within range.
	sanitizedPartitionId := positiveMod(partitionId, p.numVirtualPartitions)
	// Now, check if the mods match.
	return positiveMod(p.hostId, p.numHosts) == positiveMod(sanitizedPartitionId, p.numHosts)
}

func positiveMod(key, max int) int {
	result := key % max
	if result < 0 {
		result += max
	}
	return result
}
