/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

// modulo is a partitioner that partitions uuids based on the index of this
// partition in the total number of partitions (modulo total).
type modulo struct {
	id    uint32
	total uint32
}

func (m *modulo) IsJobManaged(uuid uint32) bool {
	return uuid%m.total == m.id
}
