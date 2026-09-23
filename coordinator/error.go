package coordinator

import (
	"errors"
	"fmt"

	sfumodels "github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// Error is a coordinator-reported error, carrying the SFU error code and
// whether the operation that produced it can be retried.
type Error struct {
	Code        int
	Message     string
	ShouldRetry bool
}

func NewError(code int, message string, shouldRetry bool) *Error {
	return &Error{
		Code:        code,
		Message:     message,
		ShouldRetry: shouldRetry,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("code: %s, message: %s", sfumodels.ErrorCode_name[int32(e.Code)], e.Message)
}

// IsRetryableError reports whether err is worth retrying. Errors the
// coordinator did not classify -- network failures, timeouts -- are retryable.
func IsRetryableError(err error) bool {
	coordErr := &Error{}
	if ok := errors.As(err, &coordErr); ok {
		return coordErr.ShouldRetry
	}
	return true
}
