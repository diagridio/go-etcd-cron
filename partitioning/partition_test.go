/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioning

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionCalculation(t *testing.T) {
	// If this test fails, it means the partitioning logic changes and it is a breaking change.
	// Users cannot run or delete their persisted jobs if this fails.
	numVirtualPartitions := 53
	numHosts := 1
	p, _ := NewPartitioning(numVirtualPartitions, numHosts, 0)
	assert.Equal(t, 14, p.CalculatePartitionId("1"))
	assert.Equal(t, 49, p.CalculatePartitionId("10"))
}

func TestPartitionLeadershipWith3Hosts(t *testing.T) {
	numVirtualPartitions := 53
	numHosts := 3
	p0, _ := NewPartitioning(numVirtualPartitions, numHosts, 0)
	p1, _ := NewPartitioning(numVirtualPartitions, numHosts, 1)
	p2, _ := NewPartitioning(numVirtualPartitions, numHosts, 2)

	assert.True(t, p0.CheckPartitionLeader(0))
	assert.False(t, p0.CheckPartitionLeader(1))
	assert.False(t, p0.CheckPartitionLeader(2))

	assert.False(t, p1.CheckPartitionLeader(0))
	assert.True(t, p1.CheckPartitionLeader(1))
	assert.False(t, p1.CheckPartitionLeader(2))

	assert.False(t, p2.CheckPartitionLeader(0))
	assert.False(t, p2.CheckPartitionLeader(1))
	assert.True(t, p2.CheckPartitionLeader(2))

	p3, p3Err := NewPartitioning(numVirtualPartitions, numHosts, 3)
	assert.Nil(t, p3)
	assert.NotNil(t, p3Err)

	p4, p4Err := NewPartitioning(numVirtualPartitions, numHosts, 4)
	assert.Nil(t, p4)
	assert.NotNil(t, p4Err)

	pNeg, pNegErr := NewPartitioning(numVirtualPartitions, numHosts, -1)
	assert.Nil(t, pNeg)
	assert.NotNil(t, pNegErr)
}

func TestPartitionLeadershipDist(t *testing.T) {

	tryAllHostsAndPartitions := func(numVirtParts, numHosts int) []int {
		count := make([]int, numHosts)
		for hostId := 0; hostId < numHosts; hostId++ {
			p, _ := NewPartitioning(numVirtParts, numHosts, hostId)
			for partitionId := 0; partitionId < numVirtParts; partitionId++ {
				if p.CheckPartitionLeader(partitionId) {
					count[hostId] = count[hostId] + 1
				}
			}
		}

		return count
	}

	t.Run("No partitioning", func(t *testing.T) {
		count := tryAllHostsAndPartitions(1, 1)
		assert.Equal(t, 1, len(count))
		assert.Equal(t, 1, count[0])
	})

	t.Run("53 partitions and 3 hosts", func(t *testing.T) {
		count := tryAllHostsAndPartitions(53, 3)
		assert.Equal(t, 3, len(count))
		assert.Equal(t, 18, count[0])
		assert.Equal(t, 18, count[1])
		assert.Equal(t, 17, count[2])
	})

	t.Run("31 partitions and 3 hosts", func(t *testing.T) {
		count := tryAllHostsAndPartitions(31, 3)
		assert.Equal(t, 3, len(count))
		assert.Equal(t, 11, count[0])
		assert.Equal(t, 10, count[1])
		assert.Equal(t, 10, count[2])
	})

	t.Run("53 partitions and 1 host", func(t *testing.T) {
		count := tryAllHostsAndPartitions(53, 1)
		assert.Equal(t, 1, len(count))
		assert.Equal(t, 53, count[0])
	})

	t.Run("3 partitions and 3 hosts", func(t *testing.T) {
		count := tryAllHostsAndPartitions(3, 3)
		assert.Equal(t, 3, len(count))
		assert.Equal(t, 1, count[0])
		assert.Equal(t, 1, count[1])
		assert.Equal(t, 1, count[2])
	})
}

func TestPartitionIdWithinRange(t *testing.T) {
	seed := int64(7) // Fixed seed value for reproducibility
	randSource := rand.NewSource(seed)
	randGen := rand.New(randSource)
	numSamples := 1000

	numVirtualPartitions := 53
	p, _ := NewPartitioning(53, 1, 0)
	maxPartitionId := numVirtualPartitions - 1

	for i := 0; i < numSamples; i++ {
		randomUUID, err := uuid.NewRandomFromReader(randGen)
		require.NoError(t, err)
		partitionId := p.CalculatePartitionId(randomUUID.String())
		assert.GreaterOrEqual(t, partitionId, 0)
		assert.LessOrEqual(t, partitionId, maxPartitionId)
	}
}

func TestNoPartitioningSingleLeader(t *testing.T) {
	partitionIdStart := -10
	partitionIdEnd := 30

	p := NoPartitioning()
	for partitionId := partitionIdStart; partitionId <= partitionIdEnd; partitionId++ {
		assert.True(t, p.CheckPartitionLeader(partitionId))
	}
}

func TestNoPartitioningSinglePartition(t *testing.T) {
	seed := int64(7) // Fixed seed value for reproducibility
	randSource := rand.NewSource(seed)
	randGen := rand.New(randSource)
	numSamples := 1000

	p := NoPartitioning()

	for i := 0; i < numSamples; i++ {
		randomUUID, err := uuid.NewRandomFromReader(randGen)
		require.NoError(t, err)
		partitionId := p.CalculatePartitionId(randomUUID.String())
		assert.Equal(t, 0, partitionId)
	}
}
