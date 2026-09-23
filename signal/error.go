package signal

import (
	"fmt"

	"github.com/GetStream/protocol/protobuf/video/sfu/models"
)

type Error struct {
	Code        models.ErrorCode
	Message     string
	ShouldRetry bool
}

func NewError(code models.ErrorCode, message string, shouldRetry bool) *Error {
	return &Error{
		Code:        code,
		Message:     message,
		ShouldRetry: shouldRetry,
	}
}

func (e *Error) Error() string {
	return fmt.Sprintf("code: %s, message: %s", models.ErrorCode_name[int32(e.Code)], e.Message)
}
