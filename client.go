package rtc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/golang-jwt/jwt/v5"
	"github.com/pion/webrtc/v4"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/atomicx"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
)

type options struct {
	coordinatorOptions     []coordinator.Option
	detectLocation         bool
	logger                 logger.ILogger
	clientDetails          ClientDetails
	statsReportingInterval time.Duration
	source                 Source

	// connectTimeout sets the maximum duration to establish the initial
	// connection (REST + WebSocket) to the coordinator. If zero, a sane
	// default (5 s) is applied.
	connectTimeout time.Duration

	withCoordinatorWS bool

	// locationHintURL is probed to discover the nearest edge. Empty means
	// DefaultLocationHintURL.
	locationHintURL string

	// iceRecoveryConfig tunes how peer connection failures are repaired before
	// the call falls back to a full reconnect.
	iceRecoveryConfig ICERecoveryConfig

	// reconnectConfig bounds how aggressively a dropped call is reconnected.
	reconnectConfig ReconnectConfig

	// healthCheckInterval and healthCheckTimeout control how quickly a silent
	// SFU is noticed. Zero means the signal package's defaults.
	healthCheckInterval time.Duration
	healthCheckTimeout  time.Duration

	// Stats providers
	audioSourceStatsProviders  []AudioSourceStatsProvider
	videoSourceStatsProviders  []VideoSourceStatsProvider
	audioPlayoutStatsProviders []AudioPlayoutStatsProvider

	// user is who NewRTCClient connects as. NewClient takes its user as an
	// argument instead.
	user User
}

type Option func(*options)

// ClientOption is an Option or a getstream.ClientOption. NewRTCClient rejects
// anything else.
type ClientOption = any

// WithUser sets the user NewRTCClient connects as. NewClient ignores it.
func WithUser(user User) Option {
	return func(o *options) {
		o.user = user
	}
}

func WithLogger(l logger.ILogger) Option {
	return func(o *options) {
		o.logger = l
		o.coordinatorOptions = append(
			o.coordinatorOptions,
			coordinator.WithLogger(l),
		)
	}
}

// WithLocationHintURL overrides the endpoint probed to find the nearest edge.
// It is only consulted when location discovery is enabled.
func WithLocationHintURL(url string) Option {
	return func(o *options) {
		o.locationHintURL = url
	}
}

func WithoutCoordinatorWS() Option {
	return func(o *options) {
		o.withCoordinatorWS = false
	}
}

// WithoutLocationDiscovery disables the automatic CloudFront-based location lookup.
func WithoutLocationDiscovery() Option {
	return func(o *options) {
		o.detectLocation = false
	}
}

func WithCoordinatorOptions(coordinatorOptions ...coordinator.Option) Option {
	return func(o *options) {
		o.coordinatorOptions = coordinatorOptions
	}
}

func WithClientDetails(clientDetails ClientDetails) Option {
	return func(o *options) {
		o.clientDetails = clientDetails
		o.coordinatorOptions = append(
			o.coordinatorOptions,
			coordinator.WithVersionHeader(clientDetails.SDKVersionString()),
		)
	}
}

func WithStatsInterval(interval time.Duration) Option {
	return func(o *options) {
		o.statsReportingInterval = interval
	}
}

func WithConnectTimeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.connectTimeout = d
		}
	}
}

func WithSource(source Source) Option {
	return func(o *options) {
		o.source = source
	}
}

type VideoOutboundRtpProvider interface {
	GetVideoOutboundRtpStats() *webrtc.OutboundRTPStreamStats
	SetVideoOutboundRtpStats(stats *webrtc.OutboundRTPStreamStats)
}

type AudioOutboundRtpProvider interface {
	GetAudioOutboundRtpStats() *webrtc.OutboundRTPStreamStats
	SetAudioOutboundRtpStats(stats *webrtc.OutboundRTPStreamStats)
}

type AudioSourceStatsProvider interface {
	GetAudioSourceStats() *webrtc.AudioSourceStats
	SetAudioSourceStats(stats *webrtc.AudioSourceStats)
	AudioOutboundRtpProvider
}

type VideoSourceStatsProvider interface {
	GetVideoSourceStats() *webrtc.VideoSourceStats
	SetVideoSourceStats(stats *webrtc.VideoSourceStats)
	VideoOutboundRtpProvider
}

type AudioPlayoutStatsProvider interface {
	GetAudioPlayoutStats() *webrtc.AudioPlayoutStats
}

func WithAudioSourceStatsProviders(providers ...AudioSourceStatsProvider) Option {
	return func(o *options) {
		o.audioSourceStatsProviders = providers
	}
}

func WithVideoSourceStatsProviders(providers ...VideoSourceStatsProvider) Option {
	return func(o *options) {
		o.videoSourceStatsProviders = providers
	}
}

func WithAudioPlayoutStatsProviders(providers ...AudioPlayoutStatsProvider) Option {
	return func(o *options) {
		o.audioPlayoutStatsProviders = providers
	}
}

type GetCredentialsFunc func(forceReload bool, excludeSFU string) (models.Credentials, error)

type Source int32

const (
	SourceWebRTC Source = iota
	SourceRTMP
	SourceSIP
	SourceSRT
	SourceRTSP
)

func (c Source) toSfuParticipantSource() sfu_models.ParticipantSource {
	switch c {
	case SourceRTMP:
		return sfu_models.ParticipantSource_PARTICIPANT_SOURCE_RTMP
	case SourceSIP:
		return sfu_models.ParticipantSource_PARTICIPANT_SOURCE_SIP
	case SourceSRT:
		return sfu_models.ParticipantSource_PARTICIPANT_SOURCE_SRT
	case SourceRTSP:
		return sfu_models.ParticipantSource_PARTICIPANT_SOURCE_RTSP
	default:
		return sfu_models.ParticipantSource_PARTICIPANT_SOURCE_WEBRTC_UNSPECIFIED
	}
}

type SDKVersion struct {
	Major string
	Minor string
	Patch string
}

type ClientDetails struct {
	SDKVersion SDKVersion
	OSName     string
	// SDKType overrides the SDK reported to the SFU. It defaults to
	// SDK_TYPE_GO, for which the SFU substitutes a fixed set of decode codecs
	// instead of the advertised ones, so tests that need their own capabilities
	// respected have to present a different SDK.
	SDKType     sfu_models.SdkType
	BrowserName string
}

// sdkType returns the SDK to report, defaulting to Go when unset.
func (c ClientDetails) sdkType() sfu_models.SdkType {
	if c.SDKType == sfu_models.SdkType_SDK_TYPE_UNSPECIFIED {
		return sfu_models.SdkType_SDK_TYPE_GO
	}
	return c.SDKType
}

func (c ClientDetails) SDKVersionString() string {
	return "stream-go-" + c.SDKVersion.Major + "." + c.SDKVersion.Minor + "." + c.SDKVersion.Patch
}

// UserType distinguishes the three ways a user can be authenticated against
// Stream. It mirrors the JS SDK's user types.
type UserType string

const (
	// UserTypeAuthenticated is a regular user identified by a JWT minted with
	// the app's secret.
	UserTypeAuthenticated UserType = "authenticated"
	// UserTypeGuest is a temporary user created on the fly by the coordinator.
	UserTypeGuest UserType = "guest"
	// UserTypeAnonymous is an unidentified viewer, only useful for watching
	// livestreams.
	UserTypeAnonymous UserType = "anonymous"
)

// User is the identity the Client connects as.
type User struct {
	ID   string
	Name string
	Type UserType
}

// TokenProvider returns a Stream JWT for the given user ID. It is called once
// when the Client is constructed.
type TokenProvider = coordinator.TokenProvider

// StaticToken returns a TokenProvider that always yields token.
func StaticToken(token string) TokenProvider {
	return coordinator.StaticTokenProvider(token)
}

type Client struct {
	apiKey            string
	token             atomicx.AtomicValue[string]
	locationCache     atomicx.AtomicValue[string]
	locationDiscovery LocationDiscovery
	server            *getstream.Stream
	options
	coordinator.CoordinatorClientInterface
	User         User
	UserID       string
	ConnectionID atomicx.AtomicValue[string]
	OwnUser      atomic.Pointer[models.OwnUserResponse]
	Tracing      atomic.Pointer[rtcstats.TraceBuffer]
	muStats      sync.RWMutex
}

func defaultClientOptions() options {
	return options{
		detectLocation:    true,
		withCoordinatorWS: true,
		logger:            logger.Noop{},
		clientDetails: ClientDetails{
			OSName: "linux",
			// todo: get version from build
			SDKVersion: SDKVersion{"0", "0", "0"},
		},
		statsReportingInterval: time.Duration(0),
		connectTimeout:         5 * time.Second,
		source:                 SourceWebRTC,
		iceRecoveryConfig:      defaultICERecoveryConfig(),
		reconnectConfig:        defaultReconnectConfig(),
		user:                   User{ID: "agent", Name: "Agent"},
	}
}

// WithICERecovery tunes how a peer connection failure is handled: how long a
// disconnected connection is given to recover on its own before ICE is
// restarted, and how many restarts are attempted before the call gives up and
// reconnects from scratch. Zero fields keep their defaults.
func WithICERecovery(cfg ICERecoveryConfig) Option {
	return func(o *options) {
		o.iceRecoveryConfig = cfg.withDefaults()
	}
}

// WithReconnectConfig tunes the reconnect loop: backoff bounds, how many fast
// reconnects are attempted, the rejoin rate limit, and the overall budget after
// which a call that cannot reconnect is abandoned. Zero fields keep their
// defaults.
func WithReconnectConfig(cfg ReconnectConfig) Option {
	return func(o *options) {
		o.reconnectConfig = cfg.withDefaults()
	}
}

// WithDisconnectionTimeout abandons a call that has been disconnected for this
// long without recovering, reporting the failure through the unretryable error
// handler. Zero, the default, retries for as long as the call is open.
//
// It is the one bound that spans a whole reconnect burst: the per-strategy caps
// and the rejoin rate limit each govern one kind of attempt, so without this a
// call can keep cycling through them indefinitely.
func WithDisconnectionTimeout(d time.Duration) Option {
	return func(o *options) {
		o.reconnectConfig.DisconnectionTimeout = d
	}
}

// WithHealthCheck overrides how often the SDK pings the SFU and how long it
// waits for a reply before treating the connection as dead and reconnecting.
// Non-positive values keep the defaults (5s and 15s).
func WithHealthCheck(interval, timeout time.Duration) Option {
	return func(o *options) {
		if interval > 0 {
			o.healthCheckInterval = interval
		}
		if timeout > 0 {
			o.healthCheckTimeout = timeout
		}
	}
}

// NewClient is the primary, client-side constructor. It mints no tokens of its
// own: token supplies the JWT for user, exactly as the JS SDK's
// StreamVideoClient takes a token or token provider.
//
// The returned Client is connected: unless WithoutCoordinatorWS is passed, the
// coordinator websocket is up and delivering events.
func NewClient(apiKey string, user User, token TokenProvider, opts ...Option) (*Client, error) {
	if apiKey == "" {
		return nil, xerr.Error("api key is required")
	}
	if token == nil {
		return nil, xerr.Error("token provider is required")
	}

	o := defaultClientOptions()
	for _, opt := range opts {
		opt(&o)
	}
	return newClient(apiKey, user, token, o)
}

// NewRTCClient is the server-side constructor, for bots, tests and
// ingress/egress-style workloads. It builds a getstream-go Stream from the API
// secret, mints a token for the WithUser user (User{ID: "agent", Name: "Agent"}
// by default) and returns a connected Client. The Stream stays reachable
// through Server.
//
// opts may mix this package's Option values with getstream.ClientOption values,
// which configure the Stream.
//
// Never ship an API secret to an end-user device; use NewClient there.
func NewRTCClient(apiKey, apiSecret string, opts ...ClientOption) (*Client, error) {
	if apiKey == "" {
		return nil, xerr.Error("api key is required")
	}
	if apiSecret == "" {
		return nil, xerr.Error("api secret is required")
	}

	o := defaultClientOptions()
	var serverOpts []getstream.ClientOption
	for _, opt := range opts {
		switch opt := opt.(type) {
		case Option:
			opt(&o)
		case getstream.ClientOption:
			serverOpts = append(serverOpts, opt)
		default:
			return nil, xerr.Errorf("unsupported option %T", opt)
		}
	}

	server, err := getstream.NewClient(apiKey, apiSecret, serverOpts...)
	if err != nil {
		return nil, xerr.Wrapf(err, "build server client")
	}

	token := func(userID string) (string, error) {
		return server.CreateToken(userID)
	}

	c, err := newClient(apiKey, o.user, token, o)
	if err != nil {
		return nil, err
	}
	c.server = server
	return c, nil
}

func newClient(apiKey string, user User, token TokenProvider, o options) (*Client, error) {
	tok, err := token(user.ID)
	if err != nil {
		return nil, xerr.Wrapf(err, "get token for user %q", user.ID)
	}

	userID := user.ID
	if userID == "" {
		// Anonymous and guest tokens carry the ID the coordinator assigned.
		userID = extractUserID(tok)
	}

	c := &Client{
		User:    user,
		UserID:  userID,
		apiKey:  apiKey,
		options: o,
	}
	// This will be the general Tracer for the SDK client, unrelated to connections and PCs
	if c.StatsReportingInterval() > 0 {
		c.Tracing.Store(rtcstats.NewClientTraceBuffer(""))
	}

	coordOptions := o.coordinatorOptions
	if !c.withCoordinatorWS {
		coordOptions = append(coordOptions, coordinator.WithoutWebsocket())
	}

	cc, err := coordinator.NewClient(apiKey, userID, coordinator.StaticTokenProvider(tok),
		coordinator.NoopHandler{}, coordOptions...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), o.connectTimeout)
	defer cancel()

	o.logger.Info("user", userID)

	if o.withCoordinatorWS {
		auth := models.WSAuthMessage{
			UserDetails: models.ConnectUserDetailsRequest{
				ID:   userID,
				Name: nonEmpty(user.Name),
			},
			Token: tok,
		}
		c.Tracing.Load().Emit(rtcstats.CoordinatorWSConnectEvent, auth)
		resp, err := connectWsWithRetries(ctx, cc, &auth)
		if err != nil {
			return nil, err
		}
		c.Tracing.Load().Emit(rtcstats.CoordinatorWSConnectedEvent, resp)
		c.ConnectionID.Store(resp.ConnectionID)
		c.OwnUser.Store(&resp.Me)
	} else {
		c.ConnectionID.Store("")
		c.OwnUser.Store(&models.OwnUserResponse{})
	}
	c.CoordinatorClientInterface = cc
	c.token.Store(tok)
	if o.detectLocation {
		c.locationDiscovery = NewCloudFrontDiscovery(o.locationHintURL, 3, locationHTTPClient, c.logger)
	}
	return c, nil
}

// Server returns the embedded server-side SDK, or nil when the Client was built
// without an API secret.
func (c *Client) Server() *getstream.Stream {
	return c.server
}

// Call constructs a handle for the given call. It performs no I/O; call
// (*Call).Join to actually join.
func (c *Client) Call(callType, id string) *Call {
	return newCall(c, callType, id)
}

func extractUserID(token string) string {
	var claims jwt.MapClaims
	if _, _, err := (&jwt.Parser{}).ParseUnverified(token, &claims); err != nil {
		return ""
	}

	if id, ok := claims["user_id"].(string); ok {
		return id
	}
	return ""
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type QueryCallsResponse struct {
	Calls []*Call
	Next  *string
	Prev  *string
}

func (c *Client) ClientDetails() ClientDetails {
	return c.clientDetails
}

func (c *Client) GetAudioSourceStatsProviders() []AudioSourceStatsProvider {
	c.muStats.RLock()
	defer c.muStats.RUnlock()
	return c.audioSourceStatsProviders
}

func (c *Client) GetVideoSourceStatsProviders() []VideoSourceStatsProvider {
	c.muStats.RLock()
	defer c.muStats.RUnlock()
	return c.videoSourceStatsProviders
}

func (c *Client) AddAudioSourceStatsProviders(providers ...AudioSourceStatsProvider) {
	c.muStats.Lock()
	defer c.muStats.Unlock()
	c.audioSourceStatsProviders = append(c.audioSourceStatsProviders, providers...)
}

func (c *Client) AddVideoSourceStatsProviders(providers ...VideoSourceStatsProvider) {
	c.muStats.Lock()
	defer c.muStats.Unlock()
	c.videoSourceStatsProviders = append(c.videoSourceStatsProviders, providers...)
}

func (c *Client) detectLocationCached(ctx context.Context) string {
	if c.locationCache.Load() == "" {
		c.logger.Info("Join call request without location, discovering location...")
		loc := c.locationDiscovery.Discover(ctx)
		c.locationCache.Store(loc)
		c.logger.Infof("%q location discovered", loc)
	}
	return c.locationCache.Load()
}

func (c *Client) connectWithRetries(
	ctx context.Context,
	_type, id string,
	joinCallRequest models.JoinCallRequest,
) (*models.JoinCallResponse, error) {
	backoff := 100 * time.Millisecond
	var lastError error

	for {
		select {
		case <-ctx.Done():
			if lastError != nil {
				return nil, xerr.Wrap(lastError)
			}
			return nil, ctx.Err()
		default:
		}

		connId := c.ConnectionID.Load()
		c.Tracing.Load().Emit(rtcstats.CoordinatorJoinCallEvent, joinCallRequest)
		result, err := c.CoordinatorClientInterface.JoinCall(ctx, _type, id, joinCallRequest, &connId)
		if err == nil {
			c.Tracing.Load().Emit(rtcstats.CoordinatorJoinCallResponseEvent, result)
			return &result, nil
		}

		lastError = err
		if !coordinator.IsRetryableError(err) {
			return nil, xerr.Wrap(err)
		}

		time.Sleep(backoff)
		// Exponential backoff with max of 1.2 seconds
		if backoff < 1200*time.Millisecond {
			backoff *= 2
			if backoff > 1200*time.Millisecond {
				backoff = 1200 * time.Millisecond
			}
		}
	}
}

// connectWsWithRetries attempts to establish the initial coordinator WebSocket
// connection, retrying transient/transport errors considered retry-able by the
// generated coordinator client. It respects the caller-supplied context.
// The back-off logic mirrors connectWithRetries.
func connectWsWithRetries(
	ctx context.Context,
	cc coordinator.CoordinatorClientInterface,
	joinReq *models.WSAuthMessage,
) (*models.ConnectedEvent, error) {
	backoff := 100 * time.Millisecond
	var lastError error
	for {
		select {
		case <-ctx.Done():
			if lastError != nil {
				return nil, xerr.Wrap(lastError)
			}
			return nil, ctx.Err()
		default:
		}

		resp, err := cc.Connect(ctx, joinReq)
		if err == nil {
			return resp, nil
		}
		lastError = err
		if !coordinator.IsRetryableError(err) {
			return nil, xerr.Wrap(err)
		}
		time.Sleep(backoff)
		if backoff < 1200*time.Millisecond {
			backoff *= 2
			if backoff > 1200*time.Millisecond {
				backoff = 1200 * time.Millisecond
			}
		}
	}
}

// joinCoordinator performs the coordinator half of a join: it resolves the
// caller's location if needed, POSTs to /join and returns both the response and
// a GetCredentialsFunc the call can use to re-fetch credentials during a
// reconnect or migration.
func (c *Client) joinCoordinator(
	ctx context.Context,
	callType, id string,
	joinCallRequest models.JoinCallRequest,
) (*models.JoinCallResponse, GetCredentialsFunc, error) {
	if joinCallRequest.Location == "" && c.detectLocation {
		joinCallRequest.Location = c.detectLocationCached(ctx)
	}

	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectEvent, joinCallRequest)
	result, err := c.connectWithRetries(ctx, callType, id, joinCallRequest)
	if err != nil {
		return nil, nil, err
	}
	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectedEvent, result)

	getCred := func(forceReload bool, excludeSFUID string) (models.Credentials, error) {
		if !forceReload && excludeSFUID == "" {
			return result.Credentials, nil
		}
		req := joinCallRequest
		if excludeSFUID != "" {
			req.MigratingFrom = &excludeSFUID
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second*5)
		defer cancel()
		c.Tracing.Load().Emit(rtcstats.CoordinatorConnectEvent, req)
		retryResult, err := c.connectWithRetries(ctx, callType, id, req)
		if err != nil {
			return models.Credentials{}, err
		}
		c.Tracing.Load().Emit(rtcstats.CoordinatorConnectedEvent, retryResult)
		return retryResult.Credentials, nil
	}

	return result, getCred, nil
}

func (c *Client) StatsReportingInterval() time.Duration {
	return c.statsReportingInterval
}
