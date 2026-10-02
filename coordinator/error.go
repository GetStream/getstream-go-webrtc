package coordinator

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Error is a coordinator-reported error, carrying the coordinator's error code and
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
	if e.Status == 0 {
		return fmt.Sprintf("code: %d, message: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("code: %d, status: %d, message: %s", e.Code, e.Status, e.Message)
}

const (
	// notFound is the coordinator's error code for a resource that does not exist.
	notFound = 16
	// tokenExpired is the coordinator's error code for an expired token.
	tokenExpired = 40
	// joinFlowFailure is the coordinator's error code for a join it could not complete,
	// such as "could not find any available server for this call, try again".
	joinFlowFailure = 101
	// fastJoinDisabled is the coordinator's error code for a fast_join it has turned off
	// for the app or the shard.
	fastJoinDisabled = 114
)

// IsJoinFlowFailure reports whether err is the coordinator failing to complete a join
// on its side, such as finding no SFU for the call.
func IsJoinFlowFailure(err error) bool {
	coordErr := &Error{}
	return errors.As(err, &coordErr) && coordErr.Code == joinFlowFailure && coordErr.Status/100 == 5
}

// IsFastJoinUnavailable reports whether err is a fast_join the deployment does not serve,
// so the client joins the legacy way instead. That is the coordinator's switch turning it
// off (code 114), or a 404 from before the coordinator's handler: a router or an edge
// without the route. A 404 from the handler itself, for a call or a user that does not
// exist, is not.
func IsFastJoinUnavailable(err error) bool {
	coordErr := &Error{}
	if !errors.As(err, &coordErr) || coordErr.Status != http.StatusNotFound {
		return false
	}
	switch coordErr.Code {
	case fastJoinDisabled:
		return true
	case notFound:
		return !fromHandler(coordErr.Message)
	}
	// Not a coordinator error at all, such as a router's plain-text 404.
	return coordErr.Code == 0
}

// fromHandler reports whether a coordinator error message comes from an endpoint's
// handler. The coordinator and its edge prefix every error with the handler's name, as
// `FastJoinCall failed with error: "..."`; the edge leaves the name empty for a path it
// has no route for.
func fromHandler(message string) bool {
	name, _, ok := strings.Cut(message, " failed with error: ")
	return ok && name != ""
}

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

// IsNotFound reports whether err is an HTTP 404: for an endpoint, that the coordinator
// or an edge in front of it does not have it, or that what it acts on does not exist.
// IsFastJoinUnavailable tells the two apart for fast_join.
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
