/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

// modulo is a partitioner that partitions based on the partition ID based on
// the index of this partition in the total number of partitions (modulo
// total).
type modulo struct {
	id    uint64
	total uint64
}

func (m *modulo) IsJobManaged(partitionID uint64) bool {
	return partitionID%m.total == m.id
}

func (m *modulo) Total() uint64 {
	return m.total
}
