/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package validator

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// maxJobNameLength bounds the job name length to keep etcd keys and storage
// from growing unbounded. It is generous: full actor reminder names
// (actorreminder||namespace||type||id||name) are typically well under this.
const maxJobNameLength = 512

// forbiddenJobNameChars are characters that must never appear in a job name.
// '/' and '\' would let a name escape the etcd key path partition; '#' and '?'
// would allow injection when the name is embedded in a URL path or query. NUL
// and other control characters are rejected separately. '||' remains reserved
// as the internal name delimiter, but a single '|' is permitted.
const forbiddenJobNameChars = "#?/\\"

// Validator validates API request payloads.
type Validator struct{}

func New() *Validator {
	return &Validator{}
}

// JobName validates a job name string. Names may contain any character except
// '/', '\', '#', '?', and control characters (including NUL and the Unicode C1
// range), and may not be the path traversal sequences "." or "..". This mirrors
// the character policy applied to names at the Dapr API edge so that anything
// accepted there can be scheduled.
//
// The original input is validated as-is; no sanitization is applied, since the
// stored etcd key uses the original name and any sanitization here would let a
// forbidden character slip through to storage.
func (v *Validator) JobName(name string) error {
	if len(name) == 0 {
		return errors.New("job name cannot be empty")
	}

	if len(name) > maxJobNameLength {
		return fmt.Errorf("job name is invalid %q: must be at most %d characters", name, maxJobNameLength)
	}

	if strings.ContainsAny(name, forbiddenJobNameChars) {
		return fmt.Errorf("job name is invalid %q: must not contain '/', '\\', '#' or '?'", name)
	}

	for _, r := range name {
		if unicode.IsControl(r) {
			return fmt.Errorf("job name is invalid %q: must not contain control characters", name)
		}
	}

	if name == "." || name == ".." {
		return fmt.Errorf("job name is invalid %q: must not be a path traversal sequence", name)
	}

	return nil
}
