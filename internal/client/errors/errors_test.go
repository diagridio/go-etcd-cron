/*
Copyright (c) 2025 Diagrid Inc.
Licensed under the MIT License.
*/

package errors

import "testing"

func Test_KeyAlreadyExists(t *testing.T) {
	var _ error = NewKeyAlreadyExists("")
}
