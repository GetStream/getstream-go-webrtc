package coordinator

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	sfumodels "github.com/GetStream/protocol/protobuf/video/sfu/models"
)

// Error is a coordinator-reported error, carrying the SFU error code and
// whether the operation that produced it can be retried.
type Error struct {
	Code        int
	Message     string
	ShouldRetry bool
	// Status is the HTTP status of the response the error came from, or zero.
	Status int
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

const (
	// notFound is the coordinator's error code for a resource that does not exist.
	notFound = 16
	// tokenExpired is the coordinator's error code for an expired token.
	tokenExpired = 40
)

// IsTokenExpired reports whether err is the coordinator refusing an expired token.
func IsTokenExpired(err error) bool {
	coordErr := &Error{}
	return errors.As(err, &coordErr) && coordErr.Code == tokenExpired
}

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

// IsRefusal reports whether err is the coordinator refusing the user or their token for
// good: a 401 or 403, or a 404 for a user that was deleted or deactivated. Asking again,
// on either join flow, is refused the same way. Code 40, an expired or revoked token, is
// left out: a new token from the token provider can change the answer.
func IsRefusal(err error) bool {
	coordErr := &Error{}
	if !errors.As(err, &coordErr) {
		return false
	}
	switch coordErr.Status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return coordErr.Code != tokenExpired
	case http.StatusNotFound:
		if coordErr.Code != notFound {
			return false
		}
		_, rest, ok := strings.Cut(coordErr.Message, `"the user `)
		return ok && (strings.HasSuffix(rest, ` was deleted"`) || strings.HasSuffix(rest, ` was deactivated"`))
	}
	return false
}

// IsNotFound reports whether err is an HTTP 404: for an endpoint, that the coordinator
// or an edge in front of it does not have it.
func IsNotFound(err error) bool {
	coordErr := &Error{}
	return errors.As(err, &coordErr) && coordErr.Status == http.StatusNotFound
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
