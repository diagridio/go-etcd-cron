/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioning

import (
	"fmt"
	"hash/fnv"
)

// The Partitioner splits the jobs between partitions.
type Partitioner interface {
	CalculatePartitionId(key string) int
	ListPartitions() []int
	CheckPartitionLeader(partitionId int) bool
}

type partitioner struct {
	// Number of hosts to compete to leadership of partitions.
	numHosts int
	// Maximum number of virtual partitions to split the jobs.
	numVirtualPartitions int
	// This hostId (must be between 0 and numHosts)
	hostId int
	// Partitions that this instance will try to become a leader for.
	myPartitions []int
}

// NoPartitioning will make any host a leader of the only partition.
func NoPartitioning() Partitioner {
	p, _ := NewPartitioning(1, 1, 0)
	return p
}

// NewPartitioning holds the logic to hold job partitioning
func NewPartitioning(numVirtualPartitions int, numHosts int, hostId int) (Partitioner, error) {
	if numVirtualPartitions < numHosts {
		return nil, fmt.Errorf("cannot have more hosts than partitions")
	}
	if hostId < 0 {
		return nil, fmt.Errorf("hostId cannot be zero")
	}
	if hostId >= numHosts {
		return nil, fmt.Errorf("hostId cannot be greater or equal to %d", numHosts)
	}
	partitions := []int{}
	for i := 0; i < numVirtualPartitions; i++ {
		if checkPartitionLeader(i, numVirtualPartitions, hostId, numHosts) {
			partitions = append(partitions, i)
		}
	}
	return &partitioner{
		numVirtualPartitions: numVirtualPartitions,
		numHosts:             numHosts,
		hostId:               hostId,
		myPartitions:         partitions,
	}, nil
}

func (p *partitioner) CalculatePartitionId(key string) int {
	// Changing this algorithm is a breaking change and stored data is no longer valid.
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(uint32(hash.Sum32()) % uint32(p.numVirtualPartitions))
}

func (p *partitioner) ListPartitions() []int {
	return p.myPartitions
}

func (p *partitioner) CheckPartitionLeader(partitionId int) bool {
	return checkPartitionLeader(partitionId, p.numVirtualPartitions, p.hostId, p.numHosts)
}

func checkPartitionLeader(partitionId, numVirtualPartitions, hostId, numHosts int) bool {
	// First, make sure partitionId is within range.
	sanitizedPartitionId := positiveMod(partitionId, numVirtualPartitions)
	// Now, check if the mods match.
	return positiveMod(hostId, numHosts) == positiveMod(sanitizedPartitionId, numHosts)
}

func positiveMod(key, max int) int {
	result := key % max
	if result < 0 {
		result += max
	}
	return result
}
