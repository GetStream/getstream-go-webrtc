// Package xerr provides the small set of error helpers this module needs.
package xerr

import (
	"errors"
	"fmt"
)

// Wrap annotates err with an optional message. A nil err returns nil so it can
// be used directly in a return statement.
func Wrap(err error, args ...any) error {
	if err == nil {
		return nil
	}
	if len(args) == 0 {
		return err
	}
	return fmt.Errorf("%v: %w", args[0], err)
}

// Wrapf annotates err with a formatted message. A nil err returns nil.
func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", fmt.Sprintf(format, args...), err)
}

// Error returns a new error with the given message.
func Error(msg string) error { return errors.New(msg) }

// Errorf returns a new formatted error.
func Errorf(format string, args ...any) error { return fmt.Errorf(format, args...) }
