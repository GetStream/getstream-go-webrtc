package pc

import (
	"fmt"

	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// NegotiationError represents a structured error from negotiation failures
type NegotiationError struct {
	// Reason is a human-readable description of what went wrong
	Reason string
	// Code is the error code from the SFU response (if available)
	Code sfu_models.ErrorCode
	// Message is the detailed error message from the SFU (if available)
	Message string
	// Underlying error (if any)
	Err error
}

func (e *NegotiationError) Error() string {
	if e.Code != sfu_models.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return fmt.Sprintf("negotiation failed: %s (code: %s, message: %s)",
			e.Reason, e.Code.String(), e.Message)
	}
	if e.Err != nil {
		return fmt.Sprintf("negotiation failed: %s: %v", e.Reason, e.Err)
	}
	return fmt.Sprintf("negotiation failed: %s", e.Reason)
}

func (e *NegotiationError) Unwrap() error {
	return e.Err
}

// NewNegotiationError creates a negotiation error with optional error details.
// If sfuErr is provided, it extracts the error code and message.
// Otherwise, if err is provided, it stores it as the underlying error.
func NewNegotiationError(reason string, err error, sfuErr *sfu_models.Error) *NegotiationError {
	negErr := &NegotiationError{
		Reason: reason,
	}

	// Prioritize SFU error if present
	if sfuErr != nil {
		negErr.Code = sfuErr.Code
		negErr.Message = sfuErr.Message
		return negErr
	}

	// Otherwise use regular error
	if err != nil {
		negErr.Err = err
	}

	return negErr
}
