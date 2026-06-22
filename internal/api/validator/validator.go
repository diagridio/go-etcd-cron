/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package validator

import (
	"errors"
	"fmt"
	"strings"
)

// maxJobNameLength bounds the job name length to keep etcd keys and storage
// from growing unbounded. It is generous: full actor reminder names
// (actorreminder||namespace||type||id||name) are typically well under this.
const maxJobNameLength = 512

// forbiddenJobNameChars are characters that must never appear in a job name.
// '/' and '\' would let a name escape the etcd key path partition; '#' and '?'
// and NUL would allow injection when the name is embedded in a URL path or
// query. '||' remains reserved as the internal name delimiter, but a single
// '|' is permitted.
const forbiddenJobNameChars = "#?\x00/\\"

// Options is a struct that contains options for the validator.
type Options struct {
	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation. Retained for API compatibility; the default validation no
	// longer depends on it.
	JobNameSanitizer *strings.Replacer
}

// Validator validates API request payloads.
type Validator struct {
	jobNameSanitizer *strings.Replacer
}

func New(opts Options) *Validator {
	jobNameSanitizer := opts.JobNameSanitizer
	if jobNameSanitizer == nil {
		jobNameSanitizer = strings.NewReplacer()
	}
	return &Validator{
		jobNameSanitizer: jobNameSanitizer,
	}
}

// JobName validates a job name string. Names may contain any character except
// '/', '\', '#', '?', NUL and control characters, and may not be the path
// traversal sequences "." or "..". This mirrors the character policy applied to
// names at the Dapr API edge so that anything accepted there can be scheduled.
func (v *Validator) JobName(name string) error {
	name = v.jobNameSanitizer.Replace(name)

	if len(name) == 0 {
		return errors.New("job name cannot be empty")
	}

	if len(name) > maxJobNameLength {
		return fmt.Errorf("job name is invalid %q: must be at most %d characters", name, maxJobNameLength)
	}

	if strings.ContainsAny(name, forbiddenJobNameChars) {
		return fmt.Errorf("job name is invalid %q: must not contain '/', '\\', '#', '?' or NUL", name)
	}

	for i := range name {
		if b := name[i]; b < 0x20 || b == 0x7f {
			return fmt.Errorf("job name is invalid %q: must not contain control characters", name)
		}
	}

	if name == "." || name == ".." {
		return fmt.Errorf("job name is invalid %q: must not be a path traversal sequence", name)
	}

	return nil
}
