package coordinator

import (
	"errors"
	"fmt"
	"strings"

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

// notFound is the coordinator's error code for a resource that does not exist.
const notFound = 16

// IsUnknownUser reports whether err is the coordinator refusing a user it has
// never seen. Only the websocket's connect creates a user from its token. The
// coordinator wraps the message as `JoinCall failed with error: "the user X does not exist"`.
func IsUnknownUser(err error) bool {
	coordErr := &Error{}
	if !errors.As(err, &coordErr) || coordErr.Code != notFound {
		return false
	}
	_, rest, ok := strings.Cut(coordErr.Message, `"the user `)
	return ok && strings.HasSuffix(rest, ` does not exist"`)
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
