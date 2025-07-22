package utils

import (
	"errors"
	"fmt"

	cid "github.com/ipfs/go-cid"
)

// WrapError wraps an error with a descriptive operation context.
// This standardizes error wrapping patterns used throughout the codebase.
func WrapError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// WrapErrorf wraps an error with a formatted descriptive operation context.
// This allows for more flexible error messaging while maintaining consistency.
func WrapErrorf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	operation := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", operation, err)
}

// CheckCID validates that a CID is defined (not cid.Undef).
// Returns an error if the CID is undefined.
func CheckCID(c cid.Cid) error {
	if c == cid.Undef {
		return errors.New("CID is undefined")
	}
	return nil
}

// CheckCIDf validates that a CID is defined with a custom error message.
func CheckCIDf(c cid.Cid, format string, args ...interface{}) error {
	if c == cid.Undef {
		return fmt.Errorf(format, args...)
	}
	return nil
}

// IsUndefinedCID checks if a CID is undefined.
func IsUndefinedCID(c cid.Cid) bool {
	return c == cid.Undef
}
