package rtc

import (
	"context"
	"math/rand/v2"
	"net/http"
	"slices"
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
	"github.com/GetStream/getstream-go-webrtc/internal/netdelay"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
)

type options struct {
	coordinatorOptions     []coordinator.Option
	logger                 logger.ILogger
	clientDetails          ClientDetails
	statsReportingInterval time.Duration
	source                 Source

	// connectTimeout sets the maximum duration to establish the initial
	// connection (REST + WebSocket) to the coordinator. If zero, a sane
	// default (5 s) is applied.
	connectTimeout time.Duration

	withCoordinatorWS bool

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

	// networkDelay is the round-trip time WithNetworkDelay adds to every connection.
	networkDelay time.Duration

	// keepWarm opens the coordinator connection in NewClient and keeps the client's
	// connections open between joins with a request every keepWarmEvery.
	keepWarm      bool
	keepWarmEvery time.Duration
}

// WithNetworkDelay makes every connection the client opens behave as if it crossed a
// network with round-trip time rtt: the coordinator REST and websocket connections, the
// SFU websocket and RPCs, and the peer connections' UDP sockets. Each
// packet is held rtt/2 when sent and rtt/2 when received, in order, and a TCP connect
// takes one rtt. It lets a test or bench against a local stack measure round trips as
// if the stack were remote.
//
// For tests and benches only.
func WithNetworkDelay(rtt time.Duration) Option {
	return func(o *options) {
		o.networkDelay = rtt
	}
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

func WithoutCoordinatorWS() Option {
	return func(o *options) {
		o.withCoordinatorWS = false
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

// TokenProvider returns a Stream JWT for the given user ID. It is called when the
// Client is constructed, and again when the coordinator refuses the token as expired
// while the websocket reconnects.
type TokenProvider = coordinator.TokenProvider

// StaticToken returns a TokenProvider that always yields token.
func StaticToken(token string) TokenProvider {
	return coordinator.StaticTokenProvider(token)
}

type Client struct {
	apiKey string
	token  atomicx.AtomicValue[string]
	server *getstream.Stream
	options
	coordinator.CoordinatorClientInterface
	User         User
	UserID       string
	ConnectionID atomicx.AtomicValue[string]
	OwnUser      atomic.Pointer[models.OwnUserResponse]
	Tracing      atomic.Pointer[rtcstats.TraceBuffer]
	muStats      sync.RWMutex

	// wsReady is closed when the coordinator websocket NewClient started in the
	// background has first connected, or failed to within the connect timeout;
	// ConnectionID is empty in the latter case. It keeps reconnecting until Close,
	// which ends wsCtx and waits for wsDone.
	wsReady  chan struct{}
	wsCtx    context.Context
	wsCancel context.CancelFunc
	wsDone   chan struct{}
	// tokenProvider renews the token when the websocket's is refused as expired.
	tokenProvider TokenProvider

	// watched holds the joined calls whose events the coordinator websocket gets, by
	// cid. Each is watched on every new connection, until its context ends. watchMu
	// also orders a call's registration against ConnectionID changes, so each call is
	// watched once per connection.
	watchMu sync.Mutex
	watched map[string]*watchedCall

	// connectTrace holds the coordinator websocket's spans once it is up. The first
	// join claims them into connectClaim, whichever of the two comes first: later joins
	// find the websocket already open.
	connectMu    sync.Mutex
	connectTrace *jointrace.Recorder
	connectClaim *jointrace.Recorder

	// knownRTT is the last round-trip time measured to each peer, for a join whose
	// requests reuse an open connection and so measure none of their own.
	rttMu    sync.Mutex
	knownRTT map[jointrace.Peer]time.Duration

	// transport carries the coordinator requests and every call's SFU RPCs, so a later
	// join reuses the connections an earlier one, or the warmer, opened.
	transport *http.Transport
	// warm keeps transport's connections open between joins; nil WithoutKeepWarm.
	warm *warmer
}

// Close closes the coordinator connections and any idle SFU connection. The
// coordinator websocket stops reconnecting before Close returns.
func (c *Client) Close() error {
	if c.wsCancel != nil {
		c.wsCancel()
		<-c.wsDone
	}
	if c.warm != nil {
		c.warm.close()
	}
	if c.transport != nil {
		c.transport.CloseIdleConnections()
	}
	return c.CoordinatorClientInterface.Close()
}

// shareRTTs keeps the round trips a join measured for later joins, and gives the join
// the ones it could not measure because its connection was already open.
func (c *Client) shareRTTs(rec *jointrace.Recorder) {
	if rec == nil {
		return
	}
	c.rttMu.Lock()
	defer c.rttMu.Unlock()
	if c.knownRTT == nil {
		c.knownRTT = make(map[jointrace.Peer]time.Duration, 2)
	}
	peer := jointrace.PeerCoordinator
	if rtt := rec.RTT(peer); rtt > 0 {
		c.knownRTT[peer] = rtt
	} else {
		rec.SetRTT(peer, c.knownRTT[peer])
	}
}

// warmedCoordinator keeps the round trip the warmer's fresh coordinator connection
// measured, for joins that find the connection open, unless one is known already.
func (c *Client) warmedCoordinator(rtt time.Duration) {
	c.rttMu.Lock()
	defer c.rttMu.Unlock()
	if c.knownRTT == nil {
		c.knownRTT = make(map[jointrace.Peer]time.Duration, 2)
	}
	if c.knownRTT[jointrace.PeerCoordinator] == 0 {
		c.knownRTT[jointrace.PeerCoordinator] = rtt
	}
}

// claimConnectTrace makes rec, the first join's trace, the one the client's own
// websocket spans go into: now if the websocket is up, else when it comes up.
func (c *Client) claimConnectTrace(rec *jointrace.Recorder) {
	if rec == nil {
		return
	}
	c.connectMu.Lock()
	if c.connectClaim != nil {
		c.connectMu.Unlock()
		return
	}
	c.connectClaim = rec
	ws := c.connectTrace
	c.connectMu.Unlock()
	copyConnectTrace(ws, rec)
}

// connected publishes the websocket's spans, to the first join's trace if one has
// claimed them, and its RTT_c to later joins.
func (c *Client) connected(ws *jointrace.Recorder) {
	c.connectMu.Lock()
	c.connectTrace = ws
	claim := c.connectClaim
	c.connectMu.Unlock()
	copyConnectTrace(ws, claim)

	if rtt := ws.RTT(jointrace.PeerCoordinator); rtt > 0 {
		c.rttMu.Lock()
		if c.knownRTT == nil {
			c.knownRTT = make(map[jointrace.Peer]time.Duration, 2)
		}
		c.knownRTT[jointrace.PeerCoordinator] = rtt
		c.rttMu.Unlock()
	}
}

// copyConnectTrace adds the websocket's spans to rec. Its RTT_c replaces the one a
// join measured on its own: a TCP connect may end at a load balancer's edge.
func copyConnectTrace(ws, rec *jointrace.Recorder) {
	if ws == nil || rec == nil {
		return
	}
	t := ws.Trace()
	for _, s := range t.Spans {
		rec.Add(s)
	}
	for peer, rtt := range t.RTT {
		rec.ReplaceRTT(peer, rtt)
	}
}

// awaitWS waits for the background websocket connect to finish and reports whether
// it connected.
func (c *Client) awaitWS(ctx context.Context) bool {
	if c.wsReady == nil {
		return false
	}
	select {
	case <-c.wsReady:
		return c.ConnectionID.Load() != ""
	case <-ctx.Done():
		return false
	}
}

type watchedCall struct {
	ctx      context.Context
	callType string
	id       string
}

// watchCall subscribes the coordinator websocket to the call's events, now if it is
// up, and again on every reconnect, until ctx ends. Events sent while no connection
// is subscribed are not delivered; the SFU's arrive on the SFU websocket.
func (c *Client) watchCall(ctx context.Context, callType, id string) {
	if !c.withCoordinatorWS {
		return
	}
	cid := callType + ":" + id
	w := &watchedCall{ctx: ctx, callType: callType, id: id}
	c.watchMu.Lock()
	c.watched[cid] = w
	connectionID := c.ConnectionID.Load()
	c.watchMu.Unlock()
	context.AfterFunc(ctx, func() {
		c.watchMu.Lock()
		defer c.watchMu.Unlock()
		if c.watched[cid] == w {
			delete(c.watched, cid)
		}
	})
	if connectionID != "" {
		go c.watch(w, connectionID)
	}
}

// wsConnected makes connectionID the websocket's and subscribes it to every watched call.
func (c *Client) wsConnected(connectionID string) {
	c.watchMu.Lock()
	c.ConnectionID.Store(connectionID)
	calls := make([]*watchedCall, 0, len(c.watched))
	for _, w := range c.watched {
		calls = append(calls, w)
	}
	c.watchMu.Unlock()
	for _, w := range calls {
		go c.watch(w, connectionID)
	}
}

func (c *Client) wsDisconnected() {
	c.watchMu.Lock()
	c.ConnectionID.Store("")
	c.watchMu.Unlock()
}

// watch subscribes connectionID to w's events. Like the JS SDK's rewatch, it tries
// three times, while neither the call nor the client is closed and the connection is
// still the client's.
func (c *Client) watch(w *watchedCall, connectionID string) {
	ctx, cancel := context.WithCancel(w.ctx)
	defer cancel()
	defer context.AfterFunc(c.wsCtx, cancel)()
	const attempts = 3
	for attempt := 1; ; attempt++ {
		err := c.WatchCall(ctx, w.callType, w.id, connectionID)
		if err == nil || ctx.Err() != nil || c.ConnectionID.Load() != connectionID {
			return
		}
		if attempt == attempts || !coordinator.IsRetryableError(err) {
			c.logger.Warnf("coordinator websocket gets no events for call %s:%s: %v", w.callType, w.id, err)
			return
		}
		if !sleepCtx(ctx, wsRetryInterval(attempt)) {
			return
		}
	}
}

func defaultClientOptions() options {
	return options{
		withCoordinatorWS: true,
		keepWarm:          true,
		keepWarmEvery:     keepWarmInterval,
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
// It returns without waiting for the network: unless WithoutCoordinatorWS is
// passed, the coordinator websocket connects in the background, and each joined
// call's events are delivered once it is up. A join does not wait for it, except
// the first join of a user the coordinator has never seen, which only the
// websocket's connect creates.
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
// by default) and returns a Client, connecting as NewClient does. The Stream stays reachable
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
		User:          user,
		UserID:        userID,
		apiKey:        apiKey,
		options:       o,
		transport:     newHTTPTransport(o.networkDelay),
		tokenProvider: token,
		watched:       map[string]*watchedCall{},
	}
	// This will be the general Tracer for the SDK client, unrelated to connections and PCs
	if c.StatsReportingInterval() > 0 {
		c.Tracing.Store(rtcstats.NewClientTraceBuffer(""))
	}

	coordOptions := append(slices.Clone(o.coordinatorOptions), coordinator.WithHTTPTransport(c.transport))
	if !c.withCoordinatorWS {
		coordOptions = append(coordOptions, coordinator.WithoutWebsocket())
	}
	if o.networkDelay > 0 {
		coordOptions = append(coordOptions, coordinator.WithDialContext(netdelay.Dialer(o.networkDelay, nil)))
	}

	cc, err := coordinator.NewClient(apiKey, userID, coordinator.StaticTokenProvider(tok),
		coordinator.NoopHandler{}, coordOptions...)
	if err != nil {
		return nil, err
	}

	o.logger.Info("user", userID)

	c.ConnectionID.Store("")
	c.OwnUser.Store(&models.OwnUserResponse{})
	c.CoordinatorClientInterface = cc
	c.token.Store(tok)
	if o.keepWarm {
		warmCoordinator := func(ctx context.Context) error {
			if w, ok := c.CoordinatorClientInterface.(interface{ Warm(context.Context) error }); ok {
				return w.Warm(ctx)
			}
			return nil
		}
		c.warm = newWarmer(c.transport, warmCoordinator, o.keepWarmEvery, o.logger)
		c.warm.onCoordinatorRTT = c.warmedCoordinator
		c.warm.start()
	}
	c.wsReady = make(chan struct{})
	if o.withCoordinatorWS {
		auth := models.WSAuthMessage{
			UserDetails: *c.connectUserDetails(),
			Token:       tok,
		}
		c.wsCtx, c.wsCancel = context.WithCancel(context.Background())
		c.wsDone = make(chan struct{})
		go c.keepWSConnected(c.wsCtx, &auth)
	} else {
		close(c.wsReady)
	}
	return c, nil
}

// keepWSConnected keeps the coordinator websocket connected until ctx ends: it
// connects, waits for the connection to go, and reconnects after a jittered backoff,
// authenticating each new connection and watching the joined calls on it. It gives up
// only when the coordinator refuses the user for good. Joins never wait for it.
func (c *Client) keepWSConnected(ctx context.Context, auth *models.WSAuthMessage) {
	defer close(c.wsDone)
	err := c.connectWS(ctx, auth)
	failures := 0
	for {
		if err == nil {
			failures = 0
			select {
			case <-c.CoordinatorClientInterface.Disconnected():
			case <-ctx.Done():
				return
			}
			c.wsDisconnected()
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("coordinator websocket disconnected, reconnecting")
		} else if ctx.Err() != nil {
			return
		} else if coordinator.IsTokenExpired(err) {
			if again, err := c.renewToken(auth); again {
				c.logger.Error("coordinator websocket stays down: the token provider returned the expired token again")
				return
			} else if err != nil {
				c.logger.Warn("coordinator websocket: cannot renew the expired token, retrying", err)
			}
		} else if !coordinator.IsRetryableError(err) {
			c.logger.Error("coordinator websocket refused, not reconnecting: no coordinator events", err)
			return
		} else {
			c.logger.Warn("coordinator websocket did not connect, retrying", err)
		}
		failures++
		if !sleepCtx(ctx, wsRetryInterval(failures)) {
			return
		}
		err = c.reconnectWS(ctx, auth)
	}
}

// renewToken asks the token provider for a new token, for the websocket and the
// coordinator requests. again reports that it returned the expired one, as a static
// token does.
func (c *Client) renewToken(auth *models.WSAuthMessage) (again bool, err error) {
	tok, err := c.tokenProvider(c.User.ID)
	if err != nil {
		return false, xerr.Wrapf(err, "get token for user %q", c.User.ID)
	}
	if tok == auth.Token {
		return true, nil
	}
	auth.Token = tok
	c.token.Store(tok)
	if s, ok := c.CoordinatorClientInterface.(interface{ SetToken(string) }); ok {
		s.SetToken(tok)
	}
	return false, nil
}

// reconnectWS opens a new coordinator websocket, within the connect timeout.
func (c *Client) reconnectWS(ctx context.Context, auth *models.WSAuthMessage) error {
	connectCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()
	c.Tracing.Load().Emit(rtcstats.CoordinatorWSConnectEvent, redacted(auth))
	resp, err := c.CoordinatorClientInterface.Connect(connectCtx, auth)
	if err != nil {
		return err
	}
	c.wsUp(ctx, resp)
	return nil
}

// wsUp publishes a new connection, unless the client was closed while it opened.
func (c *Client) wsUp(ctx context.Context, resp *models.ConnectedEvent) {
	if ctx.Err() != nil {
		_ = c.CoordinatorClientInterface.Close()
		return
	}
	c.Tracing.Load().Emit(rtcstats.CoordinatorWSConnectedEvent, resp)
	c.OwnUser.Store(&resp.Me)
	c.wsConnected(resp.ConnectionID)
}

// redacted is auth without its token, for the trace.
func redacted(auth *models.WSAuthMessage) models.WSAuthMessage {
	r := *auth
	r.Token = ""
	return r
}

// wsRetryInterval is how long to wait before reconnect attempt n, from 1: a random
// time between 0.25 and 2.5 s at first, growing by 2 s a failure up to 5 s, so clients
// a coordinator dropped together come back spread out. It is the JS SDK's retryInterval.
func wsRetryInterval(failures int) time.Duration {
	hi := min(500+failures*2000, 5000)
	lo := min(max(250, (failures-1)*2000), 5000)
	return time.Duration(lo+rand.IntN(hi-lo+1)) * time.Millisecond
}

// sleepCtx waits for d, or less if ctx ends first; it reports whether d passed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// connectWS connects the coordinator websocket the first time, within the connect
// timeout, and records its dial and auth as spans of their own that no join step
// waits for.
func (c *Client) connectWS(ctx context.Context, auth *models.WSAuthMessage) error {
	defer close(c.wsReady)
	connectCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()

	c.Tracing.Load().Emit(rtcstats.CoordinatorWSConnectEvent, redacted(auth))
	rec := jointrace.NewRecorder(time.Now())
	dialCtx := jointrace.WithStep(connectCtx, rec, jointrace.CoordWSDial, jointrace.PeerCoordinator)
	start := time.Now()
	resp, err := connectWsWithRetries(dialCtx, c.CoordinatorClientInterface, auth)
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		// Closed while the auth reply was in flight.
		_ = c.CoordinatorClientInterface.Close()
		return ctx.Err()
	}
	// The upgrade response is the first byte back; the auth exchange follows it.
	upgraded := jointrace.FirstByte(dialCtx)
	if upgraded.IsZero() {
		upgraded = start
	}
	rec.Add(jointrace.Span{
		Name: jointrace.CoordWSDial, Start: start, End: upgraded,
		Kind: jointrace.KindNet, Peer: jointrace.PeerCoordinator,
	})
	rec.Add(jointrace.Span{
		Name: jointrace.CoordWSAuth, After: []string{jointrace.CoordWSDial}, Start: upgraded, End: time.Now(),
		Kind: jointrace.KindNet, Peer: jointrace.PeerCoordinator,
	})
	// The coordinator sits behind a load balancer that accepts TCP (and TLS) at an
	// edge near the client, so the connect time is the round trip to that edge. The
	// upgrade and the auth exchange are answered by the coordinator itself: the
	// faster of the two is the round trip every coordinator request pays, plus the
	// little server time neither can shed.
	rtt := time.Duration(0)
	for _, name := range []string{jointrace.CoordWSDial + jointrace.DetailFirstByte, jointrace.CoordWSAuth} {
		if s, ok := rec.Get(name); ok && s.Duration() > 0 && (rtt == 0 || s.Duration() < rtt) {
			rtt = s.Duration()
		}
	}
	rec.ReplaceRTT(jointrace.PeerCoordinator, rtt)
	c.wsUp(ctx, resp)
	c.connected(rec)
	return nil
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

// connectUserDetails are the user details of the websocket connect, which creates the user
// if the coordinator has never seen it. fast_join sends them too, since it can get there first.
func (c *Client) connectUserDetails() *models.ConnectUserDetailsRequest {
	return &models.ConnectUserDetailsRequest{
		ID:   c.UserID,
		Name: nonEmpty(c.User.Name),
	}
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

func (c *Client) connectWithRetries(
	ctx context.Context,
	_type, id string,
	joinCallRequest models.JoinCallRequest,
) (*models.JoinCallResponse, error) {
	return retryJoin(ctx, c, joinCallRequest, func(ctx context.Context) (models.JoinCallResponse, error) {
		return c.CoordinatorClientInterface.JoinCall(ctx, _type, id, joinCallRequest)
	})
}

// retryJoin runs a coordinator join until it succeeds, retrying what IsRetryableError
// allows with a backoff. A first-ever join of a user the coordinator does not know yet
// waits once for the websocket, which is what creates the user for join, and for
// fast_join on a coordinator that does not create it from the user details.
func retryJoin[T any](
	ctx context.Context,
	c *Client,
	joinCallRequest models.JoinCallRequest,
	join func(context.Context) (T, error),
) (*T, error) {
	backoff := 100 * time.Millisecond
	var lastError error
	waitedForUser := false

	for {
		select {
		case <-ctx.Done():
			if lastError != nil {
				return nil, xerr.Wrap(lastError)
			}
			return nil, ctx.Err()
		default:
		}

		c.Tracing.Load().Emit(rtcstats.CoordinatorJoinCallEvent, joinCallRequest)
		result, err := join(ctx)
		if err == nil {
			c.Tracing.Load().Emit(rtcstats.CoordinatorJoinCallResponseEvent, result)
			return &result, nil
		}

		lastError = err
		if !waitedForUser && coordinator.IsUnknownUser(err) {
			// Only the websocket's connect creates the user: a first-ever join waits for it.
			waitedForUser = true
			if c.awaitWS(ctx) {
				continue
			}
		}
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
		if !sleepCtx(ctx, backoff) {
			return nil, xerr.Wrap(lastError)
		}
		if backoff < 1200*time.Millisecond {
			backoff *= 2
			if backoff > 1200*time.Millisecond {
				backoff = 1200 * time.Millisecond
			}
		}
	}
}

// joinCoordinator performs the coordinator half of a join: it POSTs to /join and
// returns both the response and a GetCredentialsFunc the call can use to re-fetch
// credentials during a reconnect or migration. Without a location the coordinator
// places the client by GeoIP on its address (LocationAuto).
func (c *Client) joinCoordinator(
	ctx context.Context,
	callType, id string,
	joinCallRequest models.JoinCallRequest,
	rec *jointrace.Recorder,
) (*models.JoinCallResponse, GetCredentialsFunc, error) {
	if joinCallRequest.Location == "" {
		joinCallRequest.Location = LocationAuto
	}

	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectEvent, joinCallRequest)
	start := time.Now()
	joinCtx := jointrace.WithStep(ctx, rec, jointrace.CoordJoin, jointrace.PeerCoordinator)
	result, err := c.connectWithRetries(joinCtx, callType, id, joinCallRequest)
	if err != nil {
		return nil, nil, err
	}
	note := ""
	if jointrace.Reused(joinCtx) {
		note = "reused connection"
	}
	rec.Add(jointrace.Span{
		Name:  jointrace.CoordJoin,
		Start: start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerCoordinator, Note: note,
	})
	c.shareRTTs(rec)
	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectedEvent, result)

	return result, c.legacyCredentials(ctx, callType, id, joinCallRequest, result.Credentials), nil
}

// legacyCredentials is the GetCredentialsFunc of a joined call: first the credentials the
// join handed out, then, for a reconnect or a migration, a fresh coordinator join.
func (c *Client) legacyCredentials(
	ctx context.Context,
	callType, id string,
	joinCallRequest models.JoinCallRequest,
	first models.Credentials,
) GetCredentialsFunc {
	return func(forceReload bool, excludeSFUID string) (models.Credentials, error) {
		if !forceReload && excludeSFUID == "" {
			return first, nil
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
}

// fastJoinCoordinator is joinCoordinator for the fast join: it POSTs to fast_join and
// returns the candidate SFUs instead of credentials for one.
func (c *Client) fastJoinCoordinator(
	ctx context.Context,
	callType, id string,
	joinCallRequest models.JoinCallRequest,
	rec *jointrace.Recorder,
) (*models.FastJoinCallResponse, error) {
	if joinCallRequest.Location == "" {
		joinCallRequest.Location = LocationAuto
	}

	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectEvent, joinCallRequest)
	start := time.Now()
	joinCtx := jointrace.WithStep(ctx, rec, jointrace.CoordFastJoin, jointrace.PeerCoordinator)
	req := models.FastJoinCallRequest{JoinCallRequest: joinCallRequest, UserDetails: c.connectUserDetails()}
	result, err := retryJoin(joinCtx, c, joinCallRequest, func(ctx context.Context) (models.FastJoinCallResponse, error) {
		return c.CoordinatorClientInterface.FastJoinCall(ctx, callType, id, req)
	})
	if err != nil {
		return nil, err
	}
	note := ""
	if jointrace.Reused(joinCtx) {
		note = "reused connection"
	}
	rec.Add(jointrace.Span{
		Name:  jointrace.CoordFastJoin,
		Start: start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerCoordinator, Note: note,
	})
	c.shareRTTs(rec)
	c.Tracing.Load().Emit(rtcstats.CoordinatorConnectedEvent, result)
	return result, nil
}

func (c *Client) StatsReportingInterval() time.Duration {
	return c.statsReportingInterval
}
