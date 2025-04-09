/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package errors

// KeyAlreadyExists is an error type that indicates a key already exists in the
// store.
type KeyAlreadyExists struct {
	key string
}

func (k KeyAlreadyExists) Error() string {
	return k.key
}

func NewKeyAlreadyExists(key string) KeyAlreadyExists {
	return KeyAlreadyExists{key}
}

func IsKeyAlreadyExists(err error) bool {
	_, ok := err.(KeyAlreadyExists)
	return ok
}
