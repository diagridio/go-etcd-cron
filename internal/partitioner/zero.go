/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package partitioner

// zero is a partitioner that always returns true as there is no partitioning
// of keys.
type zero struct{}

func (*zero) IsJobManaged(uint32) bool {
	return true
}
