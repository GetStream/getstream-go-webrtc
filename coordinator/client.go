// Package coordinator talks to the Stream Video coordinator: the REST call
// that joins a call and hands back SFU credentials, and the websocket that
// delivers call events.
package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/event"
	"github.com/GetStream/getstream-go-webrtc/internal/rtretry"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

// TokenProvider returns a Stream JWT for the given user.
type TokenProvider func(userID string) (string, error)

// StaticTokenProvider returns a TokenProvider that always yields the same token.
func StaticTokenProvider(token string) TokenProvider {
	return func(string) (string, error) {
		return token, nil
	}
}

type Option func(*options)

type options struct {
	apiURL        string
	wsURL         string
	tokenProvider TokenProvider
	versionHeader string
	logger        logger.ILogger
	enableWs      bool
}

var defaultOptions = options{
	apiURL:   "https://video.stream-io-api.com",
	wsURL:    "wss://video.stream-io-api.com/api/v2/connect",
	logger:   logger.Noop{},
	enableWs: true,
}

// WithoutWebsocket disables the coordinator event websocket. The join REST
// call still works; the client just receives no coordinator events.
func WithoutWebsocket() Option {
	return func(o *options) {
		o.enableWs = false
	}
}

// WithVersionHeader sets the SDK version reported to the coordinator.
func WithVersionHeader(v string) Option {
	return func(o *options) {
		o.versionHeader = v
	}
}

// ApiURL overrides the coordinator REST endpoint.
func ApiURL(apiURL string) Option {
	return func(o *options) {
		o.apiURL = apiURL
	}
}

// WithWsURL overrides the coordinator websocket endpoint.
func WithWsURL(wsURL string) Option {
	return func(o *options) {
		o.wsURL = wsURL
	}
}

// WithLogger sets the logger. Without it the client logs nothing.
func WithLogger(l logger.ILogger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// CoordinatorClientInterface is the coordinator surface the SDK depends on.
//
//go:generate go run github.com/matryer/moq@v0.5.3 -out mocks/mock_coordinator_client.go -pkg mocks . CoordinatorClientInterface
type CoordinatorClientInterface interface {
	JoinCall(
		ctx context.Context,
		_type string,
		id string,
		joinCallRequest models.JoinCallRequest,
		connectionID *string,
	) (models.JoinCallResponse, error)

	Connect(
		ctx context.Context,
		joinRequest *models.WSAuthMessage,
	) (*models.ConnectedEvent, error)

	GetInterceptor() *event.Store[models.WebsocketEvent]

	Close() error
}

// Client implements CoordinatorClientInterface.
type Client struct {
	options
	apiKey string
	userID string
	token  atomic.Pointer[string]

	*wsclient

	interceptor *event.Store[models.WebsocketEvent]
	handler     Handler
	httpClient  *http.Client
}

var _ CoordinatorClientInterface = (*Client)(nil)

func NewClient(apiKey, userID string, tokenProvider TokenProvider, handler Handler, opts ...Option) (*Client, error) {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}

	o.tokenProvider = tokenProvider
	c := &Client{
		apiKey:  apiKey,
		userID:  userID,
		options: o,
		handler: handler,
	}

	token, err := tokenProvider(userID)
	if err != nil {
		return nil, xerr.Wrapf(err, "get token for user %q", userID)
	}
	c.token.Store(&token)

	u, err := url.Parse(o.wsURL)
	if err != nil {
		return nil, xerr.Wrapf(err, "parse websocket url %q", o.wsURL)
	}
	v := u.Query()
	v.Add("api_key", apiKey)
	v.Add("user_id", userID)
	v.Add("stream-auth-type", "jwt")
	u.RawQuery = v.Encode()

	c.interceptor = event.NewStore(func(e models.WebsocketEvent) any {
		return e
	})

	if c.enableWs {
		c.wsclient = newWsClient(u.String(), c)
	}

	c.httpClient = &http.Client{
		Timeout:   5 * time.Second,
		Transport: rtretry.NewRoundTripperRetryer(http.DefaultTransport),
	}
	return c, nil
}

// JoinCall joins a call and returns the SFU credentials for it. This is the
// only coordinator endpoint the SDK calls.
func (c *Client) JoinCall(
	ctx context.Context,
	_type string,
	id string,
	joinCallRequest models.JoinCallRequest,
	connectionID *string,
) (models.JoinCallResponse, error) {
	var response models.JoinCallResponse
	err := c.makeRequest(ctx, http.MethodPost, "/api/v2/video/call/{type}/{id}/join",
		map[string]any{
			"type": _type,
			"id":   id,
		},
		map[string]any{
			"connection_id": connectionID,
		}, joinCallRequest, &response)
	return response, xerr.Wrapf(err, "join call %s:%s", _type, id)
}

func (c *Client) Close() error {
	if c.wsclient != nil {
		return c.wsclient.Close()
	}
	return nil
}

func (c *Client) makeRequest(ctx context.Context, method, path string, pathParams, queryParams map[string]any, request, response any) error {
	path = replaceTemplate(path, pathParams)
	u, err := url.Parse(c.apiURL + path)
	if err != nil {
		return xerr.Wrapf(err, "parse url %q", c.apiURL+path)
	}

	q := u.Query()
	for k, v := range queryParams {
		q.Add(k, toString(v))
	}

	q.Add("api_key", c.apiKey)
	q.Add("user_id", c.userID)
	q.Add("stream-auth-type", "jwt")
	u.RawQuery = q.Encode()

	var body io.Reader
	if request != nil {
		b, err := json.Marshal(request)
		if err != nil {
			return xerr.Wrapf(err, "marshal request")
		}
		body = bytes.NewReader(b)
	}

	r, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return xerr.Wrapf(err, "build request")
	}

	// The coordinator expects the raw JWT, not a Bearer-prefixed one.
	if token := c.token.Load(); token != nil {
		r.Header.Set("authorization", *token)
	}
	if c.versionHeader != "" {
		r.Header.Set("X-Stream-Client", c.versionHeader)
	}

	resp, err := c.httpClient.Do(r)
	if err != nil {
		return xerr.Wrapf(err, "%s %s", method, path)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return statusError(resp.StatusCode, body)
	}

	if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		// Classified rather than wrapped: a body the models cannot read will not read any
		// better on a second attempt, and an unclassified error counts as retryable, which
		// had connectWithRetries spinning on a join that had already succeeded.
		return NewError(0, fmt.Sprintf("decode response: %v", err), false)
	}
	return nil
}

// statusError turns a non-2xx response into an Error carrying the coordinator's own error
// code, retryable only when the server rather than the request is at fault.
func statusError(status int, body []byte) *Error {
	retry := status == http.StatusTooManyRequests || status/100 == 5

	var reported models.APIError
	if err := json.Unmarshal(body, &reported); err == nil && reported.Message != "" {
		return NewError(int(reported.Code), reported.Message, retry)
	}
	return NewError(0, fmt.Sprintf("unexpected status code %d: %s", status, body), retry)
}

// RawHandler forwards an event to the interceptors without going through the
// typed Handler callbacks.
func (c *Client) RawHandler(e models.WebsocketEvent) {
	if c.interceptor == nil {
		return
	}
	c.interceptor.Intercept(e)
}

// GetInterceptor returns the event store the Handle*/Await* helpers register on.
func (c *Client) GetInterceptor() *event.Store[models.WebsocketEvent] {
	return c.interceptor
}

func replaceTemplate(path string, params map[string]any) string {
	for key, value := range params {
		path = strings.ReplaceAll(path, "{"+key+"}", url.PathEscape(toString(value)))
	}
	return path
}

func toString(value any) string {
	if value == nil {
		return ""
	}

	if stringer, ok := value.(fmt.Stringer); ok {
		return stringer.String()
	}

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return ""
		}
		return toString(v.Elem().Interface())

	case reflect.String:
		return v.String()

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)

	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64)

	case reflect.Bool:
		return strconv.FormatBool(v.Bool())

	case reflect.Slice, reflect.Array:
		var result string
		for i := range v.Len() {
			if i > 0 {
				result += ", "
			}
			result += toString(v.Index(i).Interface())
		}
		return "[" + result + "]"

	case reflect.Map:
		var result string
		for _, key := range v.MapKeys() {
			if result != "" {
				result += ", "
			}
			result += fmt.Sprintf("%v: %v", toString(key.Interface()), toString(v.MapIndex(key).Interface()))
		}
		return "{" + result + "}"

	case reflect.Struct:
		return fmt.Sprintf("%#v", value)
	}

	return fmt.Sprintf("%v", value)
}

// RemoveHandler unregisters an event handler.
type RemoveHandler func()

// EventInterceptorProvider is implemented by anything that owns an event store,
// so the helpers below work with both the client and the SDK types embedding it.
type EventInterceptorProvider interface {
	GetInterceptor() *event.Store[models.WebsocketEvent]
}

// HandleEvent calls onEvent for every event of type T.
func HandleEvent[T Events](client EventInterceptorProvider, onEvent func(T)) RemoveHandler {
	handler := func(e models.WebsocketEvent) {
		if t, ok := e.(T); ok {
			onEvent(t)
		}
	}
	return RemoveHandler(client.GetInterceptor().AddInterceptor(handler))
}

// AnyCall matches events for every call.
const AnyCall = "*"

// HandleCallEvent calls onEvent for every event of type T belonging to cid, or
// to any call when cid is AnyCall.
func HandleCallEvent[T CallEvents](client EventInterceptorProvider, cid string, onEvent func(T)) RemoveHandler {
	handler := func(e models.WebsocketEvent) {
		if t, ok := e.(T); ok {
			switch cid {
			case AnyCall:
				onEvent(t)
			default:
				if getCallCid(t) == cid {
					onEvent(t)
				}
			}
		}
	}
	return RemoveHandler(client.GetInterceptor().AddInterceptor(handler))
}

// AwaitEvent returns an awaiter for the next matching event of type T.
func AwaitEvent[T Events](client EventInterceptorProvider, match func(T) bool) *event.EventAwaiter[T, models.WebsocketEvent] {
	return event.NewEventAwaiter(client.GetInterceptor(), match)
}

// AwaitCallEvent returns an awaiter for the next matching event of type T on
// the given call.
func AwaitCallEvent[T CallEvents](client EventInterceptorProvider, cid string, match func(T) bool) *event.EventAwaiter[T, models.WebsocketEvent] {
	wrapped := func(e T) bool {
		if getCallCid(e) == cid {
			return match(e)
		}
		return false
	}
	return event.NewEventAwaiter(client.GetInterceptor(), wrapped)
}
