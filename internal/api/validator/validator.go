/*
Copyright (c) 2024 Diagrid Inc.
Licensed under the MIT License.
*/

package validator

import (
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

// Options is a struct that contains options for the validator.
type Options struct {
	// JobNameSanitizer is a replacer that sanitizes job names before name
	// validation.
	JobNameSanitizer *strings.Replacer
}

// Validator validates API request payloads.
type Validator struct {
	jobNameSanitizer *strings.Replacer
}

func New(opts Options) *Validator {
	jobNameSanitizer := opts.JobNameSanitizer
	if jobNameSanitizer == nil {
		jobNameSanitizer = strings.NewReplacer("_", "", ":", "", "-", "", " ", "")
	}
	return &Validator{
		jobNameSanitizer: jobNameSanitizer,
	}
}

// JobName validates a job name string.
func (v *Validator) JobName(name string) error {
	if len(name) == 0 {
		return errors.New("job name cannot be empty")
	}

	name = v.jobNameSanitizer.Replace(name)
	for _, segment := range strings.Split(strings.ToLower(name), "||") {
		if errs := validation.IsDNS1123Subdomain(segment); len(errs) > 0 {
			return fmt.Errorf("job name is invalid %q: %s", name, strings.Join(errs, ", "))
		}
	}

	return nil
}
