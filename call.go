package rtc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/gobwas/ws"
	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/event"
	"github.com/GetStream/getstream-go-webrtc/internal/atomicx"
	"github.com/GetStream/getstream-go-webrtc/internal/netdelay"
	"github.com/GetStream/getstream-go-webrtc/internal/ratelimit"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/logger"
	"github.com/GetStream/getstream-go-webrtc/pc"
	"github.com/GetStream/getstream-go-webrtc/rtcstats"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

// JoinOption configures a single (*Call).Join.
type JoinOption func(*joinOptions)

type joinOptions struct {
	externalRTCP          bool
	sessionID             string
	subscriber            Subscriber
	reconnectDetails      *sfu_events.ReconnectDetails
	codecsFromMediaEngine bool
	subscriberPeerConfig  pc.PeerConfig
	publisherPeerConfig   pc.PeerConfig

	// preferredPublishOptions tells the SFU what this client would rather
	// publish; the SFU answers with the options it actually wants.
	preferredPublishOptions []*sfu_models.PublishOption
	// capabilities are the optional SFU behaviours this client opts into.
	capabilities []sfu_models.ClientCapability

	// coordinator half of the join request
	create        bool
	ring          bool
	notify        bool
	e2ee          bool
	membersLimit  *int32
	location      string
	migratingFrom string

	flow JoinFlow
	// tracks are published from the start: see WithTrack.
	tracks []trackWithInfo
	// audioReceiveSlots is how many receive slots a fast join asks for: see
	// WithAudioReceiveSlots.
	audioReceiveSlots uint32

	// used by integration tests to induce specific behaviour
	beforeSubscriberSendAnswer func(*signal_rpc.SendAnswerRequest) error
}

func defaultJoinOptions() joinOptions {
	return joinOptions{
		reconnectDetails: &sfu_events.ReconnectDetails{
			ReconnectAttempt: 0,
		},
		subscriber:        SubscriberFunc(func(OnTrackReceived) {}),
		create:            true,
		flow:              JoinFlowFast,
		audioReceiveSlots: DefaultAudioReceiveSlots,
	}
}

func WithSessionID(sessionID string) JoinOption {
	return func(o *joinOptions) {
		o.sessionID = sessionID
	}
}

func WithOnTrack(onTrack Subscriber) JoinOption {
	return func(o *joinOptions) {
		o.subscriber = onTrack
	}
}

func WithExternalRTCP() JoinOption {
	return func(o *joinOptions) {
		o.externalRTCP = true
	}
}

// WithCodecsFromMediaEngine populates the join request publisher and subscriber
// SDP fields from the configured media engines.
func WithCodecsFromMediaEngine() JoinOption {
	return func(o *joinOptions) {
		o.codecsFromMediaEngine = true
	}
}

func WithSubscriberPeerConfiguration(conf pc.PeerConfig) JoinOption {
	return func(o *joinOptions) {
		o.subscriberPeerConfig = conf
	}
}

func WithPublisherPeerConfiguration(conf pc.PeerConfig) JoinOption {
	return func(o *joinOptions) {
		o.publisherPeerConfig = conf
	}
}

// WithoutCreate joins only an existing call, failing if it does not exist.
// Joining creates the call by default.
func WithoutCreate() JoinOption {
	return func(o *joinOptions) {
		o.create = false
	}
}

// WithRing rings the call members on join.
func WithRing() JoinOption {
	return func(o *joinOptions) {
		o.ring = true
	}
}

// WithNotify sends a notification to the call members on join.
func WithNotify() JoinOption {
	return func(o *joinOptions) {
		o.notify = true
	}
}

// WithE2EE marks the join as end-to-end encrypted. Key exchange is the
// application's responsibility; this only tells the coordinator.
func WithE2EE() JoinOption {
	return func(o *joinOptions) {
		o.e2ee = true
	}
}

// WithMembersLimit caps how many members the coordinator returns in the call
// response.
func WithMembersLimit(limit int32) JoinOption {
	return func(o *joinOptions) {
		o.membersLimit = &limit
	}
}

// LocationAuto is the location a join sends by default: the coordinator picks the
// SFU nearest the address the request comes from (GeoIP).
const LocationAuto = "auto"

// WithLocation tells the coordinator where the client is, as an airport code
// such as "AMS", instead of LocationAuto. It is for servers that know their
// region, and for clients behind a proxy far from them.
func WithLocation(location string) JoinOption {
	return func(o *joinOptions) {
		o.location = location
	}
}

// WithMigratingFrom asks the coordinator for an edge other than the named one.
func WithMigratingFrom(sfuID string) JoinOption {
	return func(o *joinOptions) {
		o.migratingFrom = sfuID
	}
}

// WithPreferredPublishOptions asks the SFU to let this client publish with the
// given codecs, bitrates, and layer counts.
//
// It is a request, not a setting: the SFU weighs it against what the call's
// subscribers can decode and answers in the join response's publish options,
// which PublishOptions returns. Without this the SFU picks entirely on its own,
// which for a bot with a fixed encoder may not be what it can actually produce.
func WithPreferredPublishOptions(opts ...*sfu_models.PublishOption) JoinOption {
	return func(o *joinOptions) {
		o.preferredPublishOptions = clonePublishOptions(opts)
	}
}

// DefaultAudioReceiveSlots is how many audio receive slots a fast join asks for
// unless WithAudioReceiveSlots says otherwise: enough for the other side of a
// one-to-one call.
const DefaultAudioReceiveSlots = 1

// WithAudioReceiveSlots sets how many audio receive slots a fast join asks the
// SFU for; 0 asks for none. The legacy join flow never has slots.
//
// A slot is an audio m-line the SFU offers before anyone publishes into it. The
// audio of a participant who starts publishing later is bound to a free slot and
// reaches OnTrack without a renegotiation, saving its round trip; once the slots
// are used up, later audio arrives through a renegotiation as usual. Each slot
// costs an m-line in the subscriber offer, and the SFU caps how many it grants.
func WithAudioReceiveSlots(n uint32) JoinOption {
	return func(o *joinOptions) {
		o.audioReceiveSlots = n
	}
}

// WithClientCapabilities adds SFU behaviours this client opts into, on top of
// the defaults. Notably CLIENT_CAPABILITY_COORDINATOR_STATS, which lets the SFU
// forward this client's stats to the coordinator.
func WithClientCapabilities(caps ...sfu_models.ClientCapability) JoinOption {
	return func(o *joinOptions) {
		o.capabilities = append(o.capabilities, caps...)
	}
}

// clientCapabilities returns the capabilities to advertise, deduplicated.
//
// SUBSCRIBER_VIDEO_PAUSE is always included, matching the Swift SDK: it lets the
// SFU pause inbound video nobody is rendering instead of sending it and having
// it discarded. The SDK reports those pauses through
// InboundStateNotification.
func (o joinOptions) clientCapabilities() []sfu_models.ClientCapability {
	caps := []sfu_models.ClientCapability{
		sfu_models.ClientCapability_CLIENT_CAPABILITY_SUBSCRIBER_VIDEO_PAUSE,
	}
	seen := map[sfu_models.ClientCapability]bool{caps[0]: true}
	for _, capability := range o.capabilities {
		if capability == sfu_models.ClientCapability_CLIENT_CAPABILITY_UNSPECIFIED || seen[capability] {
			continue
		}
		seen[capability] = true
		caps = append(caps, capability)
	}
	return caps
}

func withReconnectDetails(details *sfu_events.ReconnectDetails) JoinOption {
	return func(o *joinOptions) {
		o.reconnectDetails = details
	}
}

func (o joinOptions) coordinatorRequest() models.JoinCallRequest {
	req := models.JoinCallRequest{
		Create:       ptrTo(o.create),
		Ring:         ptrTo(o.ring),
		Notify:       ptrTo(o.notify),
		Location:     o.location,
		MembersLimit: o.membersLimit,
	}
	if o.e2ee {
		req.E2Ee = ptrTo(true)
	}
	if o.migratingFrom != "" {
		req.MigratingFrom = ptrTo(o.migratingFrom)
	}
	return req
}

type trackWithInfo struct {
	tracks []webrtc.TrackLocal
	info   *sfu_models.TrackInfo
}

type CallConnectionState string

const (
	// CallConnectionStateConnected indicates that the client has
	// successfully connected to the SFU and the connection is healthy.
	CallConnectionStateConnected CallConnectionState = "CONNECTED"

	// CallConnectionStateConnecting indicates that the client has not
	// left the call, but currently does not have a connection to the
	// SFU. In this state, signal server RPCs may not work because the
	// client may not even have valid credentials to the SFU
	CallConnectionStateConnecting CallConnectionState = "CONNECTING"

	// CallConnectionStateMigrating indicates the client is in the process of
	// migrating to another SFU
	CallConnectionStateMigrating CallConnectionState = "MIGRATING"

	// CallConnectionStateDisconnected is a terminal state indicating that
	// the client left the call (gracefully or otherwise)
	CallConnectionStateDisconnected CallConnectionState = "DISCONNECTED"
)

type Call struct {
	Type   string
	Id     string
	UserID string

	externalRTCP bool

	mu                    sync.Mutex
	nextReconnectStrategy sfu_models.WebsocketReconnectStrategy
	onceConnect           sync.Once

	joinOptions      []JoinOption
	coordinatorState atomic.Pointer[CallState]
	// trace records the first join's steps; see JoinTrace.
	trace joinTracer
	// joinFlow is the JoinFlow the first join took.
	joinFlow atomic.Value
	// attached is closed once a fast join's websocket attach has finished, well or not.
	attached atomic.Pointer[chan struct{}]

	GetCred GetCredentialsFunc
	cred    atomic.Pointer[models.Credentials]
	// coordinator client
	cc *Client
	// signal client
	peer atomic.Pointer[peer]

	SessionID atomicx.AtomicValue[string]
	// unifiedSessionId survives the rejoins that rotate SessionID, so the server
	// can correlate stats across a client's whole time in the call.
	unifiedSessionId atomicx.AtomicValue[string]

	store atomic.Pointer[ParticipantStore]

	connState  atomicx.AtomicValue[CallConnectionState]
	callCtx    context.Context
	callCancel context.CancelFunc

	logger logger.ILogger

	publishedTracksMu  sync.Mutex
	publishedTracks    []trackWithInfo
	subscribedTracksMu sync.Mutex
	// subscribedTracks is what the application wants; sentSubscriptions is what
	// the current SFU session has been told. They diverge after a reconnect,
	// which is why the dedup check reads the latter.
	subscribedTracks  []*signal_rpc.TrackSubscriptionDetails
	sentSubscriptions []*signal_rpc.TrackSubscriptionDetails

	reconnectAttempt atomic.Uint32

	// fastReconnectDeadlineSeconds is how long the SFU promises to hold this
	// session after the websocket drops, from the JoinResponse. Zero until the
	// first successful join.
	fastReconnectDeadlineSeconds atomic.Int32
	// fastReconnectAttempts counts consecutive fast reconnects; reset once the
	// call is healthy again.
	fastReconnectAttempts atomic.Uint32
	// disconnectedSinceNanos is when the current outage started, or zero while
	// connected. Used to decide whether a fast reconnect is still in time.
	disconnectedSinceNanos atomic.Int64
	// rejoinLimiter bounds how often the expensive strategies may run, so a
	// call that cannot recover stops hammering the coordinator.
	rejoinLimiter *ratelimit.SlidingWindow

	// publishOptions is the encoding configuration the SFU last asked for. The
	// SDK does not encode, so these are handed to the application.
	publishOptionsMu      sync.Mutex
	publishOptions        []*sfu_models.PublishOption
	publishOptionsHandler func([]*sfu_models.PublishOption)

	publishQualityMu      sync.Mutex
	publishQualityHandler func([]PublishQualityTarget)

	inboundStateMu      sync.Mutex
	inboundStateHandler func([]InboundTrackState)

	statsMetrics *CallStatsReportingMetrics

	migrateRequest chan struct{}

	// runReconnect dispatches one reconnect strategy. monitorHealth goes
	// through this field rather than calling reconnect directly so a test can
	// drive the loop -- strategy selection, escalation, backoff -- with the
	// strategy bodies replaced by stubs.
	runReconnect func(context.Context, sfu_models.WebsocketReconnectStrategy) error

	// fastReconnectViable reports whether a fast reconnect can still work. It is
	// a field for the same reason as runReconnect: it inspects live peer
	// connections, which the loop tests do not have.
	fastReconnectViable func() (bool, string)

	// reconnectBackoff is the first wait between failed reconnect attempts and
	// livenessPoll how often monitorHealth re-checks a healthy connection.
	// They are fields so those tests do not have to wait in real time.
	reconnectBackoff time.Duration
	livenessPoll     time.Duration

	unretryableErrorHandler            func(error)
	publisherNegotiationFailedHandler  func(err *pc.NegotiationError)
	subscriberNegotiationFailedHandler func(err *pc.NegotiationError)

	// Tracer for API Calls
	Tracing atomic.Pointer[rtcstats.TraceBuffer]

	// used for rtcstats delta compression
	prevPublisherRtcStats  map[string]any
	prevSubscriberRtcStats map[string]any
}

// newCall builds a call handle. It performs no I/O: the coordinator join and
// the SFU connection both happen in Join.
func newCall(cc *Client, callType, callID string) *Call {
	c := &Call{
		UserID:           cc.UserID,
		migrateRequest:   make(chan struct{}),
		cc:               cc,
		Type:             callType,
		Id:               callID,
		logger:           cc.logger.WithField("cid", callType+":"+callID).WithField("user", cc.UserID),
		statsMetrics:     NewCallStatsReportingMetrics(),
		reconnectBackoff: cc.reconnectConfig.InitialBackoff,
		livenessPoll:     livenessPollInterval,
		rejoinLimiter:    newRejoinLimiter(cc.reconnectConfig),
	}
	c.runReconnect = c.reconnect
	c.fastReconnectViable = c.canFastReconnect
	c.trace.udpRTT = c.selectedPairRTT
	c.callCtx, c.callCancel = context.WithCancel(context.Background())
	c.connState.Store(CallConnectionStateConnecting)
	c.nextReconnectStrategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
	if cc.StatsReportingInterval() > 0 {
		c.Tracing.Store(cc.Tracing.Load())
	}
	c.peer.Store(&peer{})
	return c
}

// joinCoordinator runs the coordinator half of the join and brings up the
// signal client. It is a no-op after the first successful join, so the
// reconnect paths can call Join freely without re-POSTing to /join.
func (c *Call) joinCoordinator(ctx context.Context, options joinOptions) error {
	if c.GetCred != nil {
		return nil
	}

	result, getCred, err := c.cc.joinCoordinator(ctx, c.Type, c.Id, options.coordinatorRequest(), c.trace.recorder())
	if err != nil {
		return xerr.Wrap(err)
	}
	c.GetCred = getCred
	c.cc.watchCall(c.callCtx, c.Type, c.Id)

	cred := result.Credentials
	// Store the coordinator state before building the signal client: the
	// client's tracing is gated on the StatsOptions this response carries.
	c.coordinatorState.Store(&CallState{
		JoinCallRequest: ptrTo(options.coordinatorRequest()),
		Url:             cred.Server.URL,
		Token:           cred.Token,
		WebsocketUrl:    cred.Server.WsEndpoint,
		EdgeName:        cred.Server.EdgeName,
		CallResponse:    result.Call,
		Members:         result.Members,
		OwnCapabilities: result.OwnCapabilities,
		StatsOptions:    result.StatsOptions,
		Membership:      result.Membership,
	})

	c.getPeer().client.Store(signal.NewClient(cred, c, c.signalOptions()...))
	c.SetCredentials(cred)
	return nil
}

func (c *Call) CID() string {
	return c.Type + ":" + c.Id
}

// isJoinErrorRetryable checks if an error from the initial Join attempt should be retried.
// It returns true for:
// - Network/connection errors (timeout, connection refused, etc.)
// - WebSocket handshake errors with HTTP > 500
// - Coordinator errors with ShouldRetry flag set
// - Signal errors
func (c *Call) isJoinErrorRetryable(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	} else if errors.Is(err, context.Canceled) {
		return false
	}

	// Check for network errors
	var netErr net.Error
	if errors.As(err, &netErr) {
		// Network errors are generally retryable (connection refused, timeout, etc.)
		return true
	}

	// Check for WebSocket handshake failures (e.g. HTTP 503 from the SFU)
	var wsStatusErr ws.StatusError
	if errors.As(err, &wsStatusErr) {
		return int(wsStatusErr) > 500
	}

	coordErr := &coordinator.Error{}
	if ok := errors.As(err, &coordErr); ok {
		return coordErr.ShouldRetry
	}

	signalErr := &signal.Error{}
	if ok := errors.As(err, &signalErr); ok {
		// return true here, shouldRetry seems to not be reliable
		// and if we have a SFU response then we can retry
		return true
	}

	return false
}

// retryFirstJoin attempts to connect to the SFU with retry logic for the initial join.
// It uses exponential backoff similar to connectWithRetries in client.go.
func (c *Call) retryFirstJoin(ctx context.Context, firstError error) (*sfu_events.JoinResponse, error) {
	nbAttempt := 20 // around 15sec
	backoff := 10 * time.Millisecond
	lastError := firstError

	for i := nbAttempt; i > 0; i-- {
		if !c.isJoinErrorRetryable(lastError) {
			break
		}

		c.logger.Warnf("Join failed with error %v, retrying in %v", lastError.Error(), backoff)

		select {
		case <-ctx.Done():
			if lastError != nil {
				return nil, xerr.Wrap(lastError)
			}
			return nil, ctx.Err()
		case <-time.After(backoff):
		}

		// Exponential backoff with max of 1 second
		backoff = min(2*backoff, 1000*time.Millisecond)

		// Get the current reconnect strategy in a thread-safe manner
		c.mu.Lock()
		strategy := c.nextReconnectStrategy
		c.mu.Unlock()

		prevSFUID := c.coordinatorState.Load().EdgeName
		excludeSfu := prevSFUID

		// If strategy differs from Migrate, force it to rejoin
		if strategy != sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE {
			excludeSfu = ""
			strategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
		}

		cred, err := c.GetCred(true, excludeSfu)
		if err != nil {
			lastError = err
			continue
		}

		c.SetCredentials(cred)
		resp, err := c.Join(ctx, c.getReconnectOptions(strategy, prevSFUID)...)
		if err == nil {
			return resp, nil
		}

		lastError = err
	}
	return nil, xerr.Wrap(lastError)
}

func (c *Call) reconnect(ctx context.Context, reconnectStrategy sfu_models.WebsocketReconnectStrategy) error {
	c.logger.WithField("reconnect_strategy", reconnectStrategy.String()).Warn("reconnecting")
	switch reconnectStrategy {
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST:
		if err := c.fastReconnect(ctx); err != nil {
			return xerr.Wrap(err)
		}
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN:
		if err := c.rejoinReconnect(ctx); err != nil {
			return xerr.Wrap(err)
		}
	case sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE:
		if err := c.migrateReconnect(ctx); err != nil {
			return xerr.Wrap(err)
		}
	default:
		c.logger.WithField("reconnect_strategy", reconnectStrategy).Error("unknown reconnect strategy")
	}
	return nil
}

func (c *Call) getReconnectOptions(strategy sfu_models.WebsocketReconnectStrategy, fromSFUID string) []JoinOption {
	opts := slices.Clone(c.joinOptions)
	var prevSessionID string
	if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN {
		prevSessionID = c.SessionID.Load()
		opts = append(opts, WithSessionID(uuid.New().String()))

	}
	opts = append(opts, withReconnectDetails(&sfu_events.ReconnectDetails{
		Strategy:          strategy,
		AnnouncedTracks:   c.getAnnouncedTracks(),
		Subscriptions:     c.getSubscriptions(),
		ReconnectAttempt:  c.reconnectAttempt.Add(1),
		FromSfuId:         fromSFUID,
		PreviousSessionId: prevSessionID,
	}))
	return opts
}

func (c *Call) SubscriberPC() *webrtc.PeerConnection {
	sub := c.subscriberPeer()
	if sub == nil {
		return nil
	}
	return sub.PC
}

func (c *Call) PublisherPC() *webrtc.PeerConnection {
	pub := c.publisherPeer()
	if pub == nil {
		return nil
	}
	return pub.PC
}

func (c *Call) migrateReconnect(ctx context.Context) error {
	prevSFUID := c.coordinatorState.Load().EdgeName
	fromSFU := c.cred.Load()
	cred, err := c.GetCred(true, prevSFUID)
	if err != nil {
		return xerr.Wrap(err)
	}
	return c.migrate(ctx, *fromSFU, cred)
}

func (c *Call) rejoinReconnect(ctx context.Context) error {
	prevSFUID := c.coordinatorState.Load().EdgeName
	cred, err := c.GetCred(true, "")
	if err != nil {
		return xerr.Wrap(err)
	}
	c.SetCredentials(cred)
	_, err = c.Join(ctx, c.getReconnectOptions(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN, prevSFUID)...)
	if err != nil {
		return xerr.Wrap(err)
	}
	return c.restorePublishedAndSubscribedTracks()
}

func (c *Call) fastReconnect(ctx context.Context) error {
	prevSFUID := c.coordinatorState.Load().EdgeName
	_, err := c.Join(ctx, c.getReconnectOptions(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST, prevSFUID)...)
	if err != nil {
		return xerr.Wrap(err)
	}
	return nil
}

// migrate moves the call to a different SFU without interrupting media.
//
// The old peer connections keep sending and receiving throughout: they are only
// detached, so they stop driving the call's state while their media continues to
// flow. The new SFU is joined alongside them, tracks are re-announced there, and
// only once the old SFU confirms with ParticipantMigrationComplete is the old
// peer torn down. Tearing it down first -- which is what this used to do --
// leaves a hole in the media for the whole duration of the new join.
//
// Mirrors the JS SDK's Call.reconnectMigrate and the Swift SDK's MigratingStage.
func (c *Call) migrate(ctx context.Context, fromCred, targetCred models.Credentials) error {
	previous := c.getPeer()

	// Register the awaiter against the old SFU before anything else: it is the
	// old SFU that reports the migration complete, and it may do so before the
	// new join has even returned.
	var migrationComplete *event.EventAwaiter[*sfu_events.SfuEvent_ParticipantMigrationComplete, *sfu_events.SfuEvent]
	if previous != nil {
		if prevClient := previous.client.Load(); prevClient != nil {
			migrationComplete = awaitMigrationCompleteOn(prevClient)
		}
		previous.detach()
	}

	c.connState.Store(CallConnectionStateMigrating)

	// A fresh peer with its own signalling client, so the old websocket is never
	// repointed out from under its own read loop.
	next := &peer{}
	next.client.Store(signal.NewClient(targetCred, c, c.signalOptions()...))
	c.peer.Store(next)
	c.SetCredentials(targetCred)

	if err := c.migrateToNewSFU(ctx, fromCred, previous, migrationComplete); err != nil {
		// The new SFU did not work out. Drop whatever peer is current -- the join
		// may have replaced next with one carrying real peer connections -- and
		// let the caller escalate to a rejoin. The old peer is already detached
		// and cannot be revived.
		if current := c.getPeer(); current != nil {
			current.release()
		} else {
			next.release()
		}
		if previous != nil {
			previous.release()
		}
		return xerr.Wrap(err)
	}

	// Confirmed on the new SFU, so the old one has nothing left to carry.
	if previous != nil {
		previous.release()
	}
	c.connState.Store(CallConnectionStateConnected)
	return nil
}

// migrateToNewSFU performs the half of the migration that can fail, so migrate
// has a single place to clean up.
func (c *Call) migrateToNewSFU(
	ctx context.Context,
	fromCred models.Credentials,
	previous *peer,
	migrationComplete *event.EventAwaiter[*sfu_events.SfuEvent_ParticipantMigrationComplete, *sfu_events.SfuEvent],
) error {
	opts := c.getReconnectOptions(
		sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE,
		fromCred.Server.EdgeName,
	)
	if _, err := c.Join(ctx, opts...); err != nil {
		return xerr.Wrap(err)
	}
	if err := c.restorePublishedAndSubscribedTracks(); err != nil {
		return xerr.Wrap(err)
	}

	// Without a previous peer there is nobody to confirm the migration -- this
	// only happens if migrate ran before a first join, which the reconnect loop
	// does not do, but the join above already succeeded so there is nothing to
	// wait for.
	if previous == nil || migrationComplete == nil {
		return nil
	}
	if _, err := migrationComplete.Await(c.cc.reconnectConfig.MigrationCompleteTimeout); err != nil {
		return xerr.Wrapf(err, "sfu did not confirm the migration")
	}
	return nil
}

// awaitMigrationCompleteOn waits for the given SFU connection to confirm the
// migration finished.
//
// The type parameter has to be the SfuEvent oneof wrapper: signal's event store
// keys on SfuEvent.GetEventPayload(), so an awaiter for the inner
// ParticipantMigrationComplete message never matches and every MIGRATE burns its
// timeout and escalates to REJOIN.
func awaitMigrationCompleteOn(client *signal.Client) *event.EventAwaiter[*sfu_events.SfuEvent_ParticipantMigrationComplete, *sfu_events.SfuEvent] {
	return signal.AwaitEvent(client, func(*sfu_events.SfuEvent_ParticipantMigrationComplete) bool {
		return true
	})
}

// UseSFU points the call straight at an SFU with credentials the caller already holds, so
// Join skips the coordinator's join-call request.
//
// It is for development and tests against a locally running SFU, where there is no
// coordinator to hand out an SFU and a token. Production calls get both from the
// coordinator and should not use it. Call it before Join. Reconnects keep using the same
// credentials, since there is no coordinator to ask for new ones.
func (c *Call) UseSFU(cred models.Credentials) {
	c.GetCred = func(bool, string) (models.Credentials, error) { return cred, nil }
	// The reconnect and migration paths read the SFU they were on from here.
	c.coordinatorState.Store(&CallState{
		Url:          cred.Server.URL,
		Token:        cred.Token,
		WebsocketUrl: cred.Server.WsEndpoint,
		EdgeName:     cred.Server.EdgeName,
	})
	c.getPeer().client.Store(signal.NewClient(cred, c, c.signalOptions()...))
	c.SetCredentials(cred)
}

func (c *Call) SetCredentials(cred models.Credentials) {
	// The token and the TURN passwords are credentials: never log them.
	c.logger.Debugf("setting credentials for %s (%s)", cred.Server.EdgeName, cred.Server.URL)
	state := c.coordinatorState.Load()
	if state == nil {
		state = &CallState{}
	}
	state.EdgeName = cred.Server.EdgeName
	state.WebsocketUrl = cred.Server.WsEndpoint
	state.Token = cred.Token
	state.Url = cred.Server.URL
	c.coordinatorState.Store(state)
	c.cred.Store(&cred)
	c.Client().SetCredentials(cred)
}

func (c *Call) Client() *signal.Client {
	return c.peer.Load().client.Load()
}

// credentials are the SFU credentials in use, or none yet: a fast join builds its peer
// connections before the coordinator has said which SFU to use.
func (c *Call) credentials() models.Credentials {
	if cred := c.cred.Load(); cred != nil {
		return *cred
	}
	return models.Credentials{}
}

func iceServers(servers []models.ICEServerResponse) []webrtc.ICEServer {
	var out []webrtc.ICEServer
	for _, s := range servers {
		out = append(out, webrtc.ICEServer{URLs: s.Urls, Username: s.Username, Credential: s.Password})
	}
	return out
}

// unifiedSessionID returns an ID that stays the same for the lifetime of this
// Call, across every reconnect and migration, unlike SessionID which a rejoin
// rotates. It is what lets server-side stats be stitched back together into one
// timeline.
func (c *Call) unifiedSessionID() string {
	id, _ := c.unifiedSessionId.LoadOrStore(func() (string, error) {
		return uuid.New().String(), nil
	})
	return id
}

func (c *Call) GetState() *CallState {
	return c.coordinatorState.Load()
}

func ptrTo[T any](v T) *T {
	return &v
}

// unsafeString views b as a string without copying. Callers must not mutate b
// afterwards; every use here reads from a buffer that is only recycled after the
// resulting string has been marshalled onto the wire.
func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// humanizeSdkType maps an SDK enum to the name the SFU records in its stats
// pipeline. These strings are wire-visible through SendStatsRequest.Sdk, so they
// must not be reworded.
func humanizeSdkType(sdkType sfu_models.SdkType) string {
	switch sdkType {
	case sfu_models.SdkType_SDK_TYPE_FLUTTER:
		return "stream-flutter"
	case sfu_models.SdkType_SDK_TYPE_ANDROID:
		return "stream-android"
	case sfu_models.SdkType_SDK_TYPE_ANGULAR:
		return "stream-angular"
	case sfu_models.SdkType_SDK_TYPE_REACT:
		return "stream-react"
	case sfu_models.SdkType_SDK_TYPE_REACT_NATIVE:
		return "stream-react-native"
	case sfu_models.SdkType_SDK_TYPE_IOS:
		return "stream-ios"
	case sfu_models.SdkType_SDK_TYPE_UNITY:
		return "stream-unity"
	case sfu_models.SdkType_SDK_TYPE_GO:
		return "stream-go"
	}
	return sdkType.String()
}

// Join joins the call: the coordinator request, the SFU join and both peer
// connections, returning once the SFU has accepted the client. Media starts flowing
// shortly after, as ICE and DTLS complete.
//
// By default it takes the fast join (JoinFlowFast): see WithJoinFlow. The reconnect
// and migration paths re-enter Join to rebuild the SFU side against fresh
// credentials, always through the SFU websocket's JoinRequest.
func (c *Call) Join(ctx context.Context, opts ...JoinOption) (*sfu_events.JoinResponse, error) {
	options := defaultJoinOptions()
	for _, o := range opts {
		o(&options)
	}
	reconnecting := options.reconnectDetails.GetStrategy() != sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_UNSPECIFIED
	rec := c.trace.begin(time.Now(), reconnecting)
	if !reconnecting {
		c.cc.claimConnectTrace(rec)
	}

	if !reconnecting && c.GetCred == nil && options.flow == JoinFlowFast {
		resp, err := c.fastJoin(ctx, opts, options, rec)
		if !errors.Is(err, errFastJoinUnavailable) {
			return resp, err
		}
		c.logger.WithField("err", err).Warn("fast join unavailable, joining the legacy way")
	}

	resp, err := c.legacyJoin(ctx, opts, options, rec, reconnecting)
	if err != nil || reconnecting {
		return resp, err
	}
	c.joinFlow.Store(JoinFlowLegacy)
	for _, t := range options.tracks {
		if _, err := c.AddTrack(t.info, t.tracks[0]); err != nil {
			return nil, xerr.Wrap(err)
		}
	}
	return resp, nil
}

// setSessionID picks the session ID of a join: the one asked for, or else the current
// one, or else a new one.
func (c *Call) setSessionID(options joinOptions) {
	if options.sessionID != "" {
		// always overwrite if sessionID is provided
		c.SessionID.Store(options.sessionID)
	} else if c.SessionID.Load() == "" {
		c.SessionID.Store(uuid.New().String())
	}
}

// rememberJoinOptions keeps the first join's options, which the reconnects reuse.
func (c *Call) rememberJoinOptions(opts []JoinOption) {
	if c.joinOptions == nil {
		if opts == nil {
			// nil vs empty slice, todo better approach
			opts = []JoinOption{}
		}
		c.joinOptions = opts
	}
}

// joinRequest is the SFU websocket's JoinRequest for the current session and
// credentials.
func (c *Call) joinRequest(options joinOptions, publisherSDP, subscriberSDP string) *sfu_events.JoinRequest {
	return &sfu_events.JoinRequest{
		Token:            c.credentials().Token,
		SessionId:        c.SessionID.Load(),
		PublisherSdp:     publisherSDP,
		ClientDetails:    c.sfuClientDetails(),
		ReconnectDetails: options.reconnectDetails,
		Source:           c.cc.source.toSfuParticipantSource(),
		SubscriberSdp:    subscriberSDP,
		// SessionId rotates on every rejoin, so without this the server cannot
		// tell that the sessions before and after an outage were the same call
		// from the same client, and its stats are split across them.
		UnifiedSessionId:        c.unifiedSessionID(),
		Capabilities:            options.clientCapabilities(),
		PreferredPublishOptions: options.preferredPublishOptions,
	}
}

func (c *Call) sfuClientDetails() *sfu_models.ClientDetails {
	return &sfu_models.ClientDetails{
		Sdk: &sfu_models.Sdk{
			Type:  c.cc.clientDetails.sdkType(),
			Major: c.cc.clientDetails.SDKVersion.Major,
			Minor: c.cc.clientDetails.SDKVersion.Minor,
			Patch: c.cc.clientDetails.SDKVersion.Patch,
		},
		Os: &sfu_models.OS{
			Name: c.cc.clientDetails.OSName,
		},
		Browser: &sfu_models.Browser{
			Name: c.cc.clientDetails.BrowserName,
		},
	}
}

// startTracing starts the call's stats trace buffers, when the call reports stats.
func (c *Call) startTracing() {
	if !c.statsEnabled() {
		return
	}
	attempt := int64(c.reconnectAttempt.Load()) - 1
	c.Tracing.Store(rtcstats.NewCallTraceBuffer("", attempt, c.credentials().Server.EdgeName))
	c.Client().Tracing.Store(c.Tracing.Load())
}

// legacyJoin is the join through the coordinator's join and the SFU websocket's
// JoinRequest, which every reconnect takes.
func (c *Call) legacyJoin(
	ctx context.Context, opts []JoinOption, options joinOptions, rec *jointrace.Recorder, reconnecting bool,
) (*sfu_events.JoinResponse, error) {
	if err := c.joinCoordinator(ctx, options); err != nil {
		return nil, err
	}
	c.setSessionID(options)
	c.rememberJoinOptions(opts)
	c.externalRTCP = options.externalRTCP

	publisherSDP := ""
	subscriberSDP := ""
	var err error
	if options.codecsFromMediaEngine {
		publisherSDP, err = publisherJoinSDPFromMediaEngine(options.publisherPeerConfig, c.logger)
		if err != nil {
			return nil, xerr.Wrap(err)
		}
		subscriberSDP, err = subscriberJoinSDPFromMediaEngine(options.subscriberPeerConfig)
		if err != nil {
			return nil, xerr.Wrap(err)
		}
	}

	req := c.joinRequest(options, publisherSDP, subscriberSDP)
	c.startTracing()

	pcsStart := time.Now()
	if err := c.initPubAndSub(options); err != nil {
		return nil, xerr.Wrap(err)
	}
	rec.Add(jointrace.Span{
		Name: jointrace.PCsCreate, After: afterFirst(rec, jointrace.CoordJoin),
		Start: pcsStart, End: time.Now(), Kind: jointrace.KindLocal, Peer: jointrace.PeerLocal,
	})

	var resp *sfu_events.JoinResponse
	dialStart := time.Now()
	resp, err = c.Client().Connect(jointrace.WithStep(ctx, rec, jointrace.SFUWSDial, jointrace.PeerSFU), req)
	if err != nil {
		// If ReconnectStrategy is still not specified here, it means the first join failed and we did not start the retry mechanism.
		// We need to start it now, in the other case, we just need to return the error
		if options.reconnectDetails.GetStrategy() == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_UNSPECIFIED {
			resp, err = c.retryFirstJoin(ctx, err)
		}
		if err != nil {
			return nil, xerr.Wrap(err)
		}
	}
	joinedAt := time.Now()
	dialedAt := c.Client().DialedAt()
	rec.Add(jointrace.Span{
		Name: jointrace.SFUWSDial, After: afterFirst(rec, jointrace.PCsCreate),
		Start: dialStart, End: dialedAt, Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
	})
	rec.Add(jointrace.Span{
		Name: jointrace.SFUJoin, After: []string{jointrace.SFUWSDial},
		Start: dialedAt, End: joinedAt, Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
	})
	if !reconnecting {
		c.trace.markJoined()
	}
	c.store.Store(NewParticipantStore(c, resp.CallState))
	c.applyJoinResponse(resp)

	if options.reconnectDetails.GetStrategy() == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST {
		if err := c.restoreICE(ctx); err != nil {
			return nil, xerr.Wrap(err)
		}
	}

	c.onceConnect.Do(func() {
		go c.webrtcStatsWorker()
		go c.monitorHealth()
	})
	return resp, nil
}

// statsInterval returns how often to report stats to the SFU, or zero when
// reporting is off.
//
// The coordinator decides this per call through JoinCallResponse.StatsOptions,
// which is why stats used to be silently disabled for every application that did
// not pass WithStatsInterval. An explicit interval still wins, so an application
// can opt in or out regardless of the server's default.
func (c *Call) statsInterval() time.Duration {
	if explicit := c.cc.StatsReportingInterval(); explicit > 0 {
		return explicit
	}
	state := c.coordinatorState.Load()
	if state == nil || !state.StatsOptions.EnableRtcStats {
		return 0
	}
	if ms := state.StatsOptions.ReportingIntervalMs; ms > 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return 0
}

// statsEnabled reports whether anything should be traced for this call. Building
// trace buffers is only worth it when the traces will actually be reported.
func (c *Call) statsEnabled() bool {
	return c.statsInterval() > 0
}

// applyJoinResponse records the parts of the SFU's join response the SDK acts
// on later: how long a fast reconnect stays viable, and the publish options the
// SFU wants this client to encode with.
func (c *Call) applyJoinResponse(resp *sfu_events.JoinResponse) {
	if deadline := resp.GetFastReconnectDeadlineSeconds(); deadline > 0 {
		c.fastReconnectDeadlineSeconds.Store(deadline)
	}
	c.setPublishOptions(resp.GetPublishOptions(), "join response")
}

func (c *Call) getAnnouncedTracks() []*sfu_models.TrackInfo {
	c.publishedTracksMu.Lock()
	defer c.publishedTracksMu.Unlock()
	tracks := make([]*sfu_models.TrackInfo, len(c.publishedTracks))
	for i := range tracks {
		tracks[i] = proto.Clone(c.publishedTracks[i].info).(*sfu_models.TrackInfo)
	}
	return tracks
}

func (c *Call) getSubscriptions() []*signal_rpc.TrackSubscriptionDetails {
	c.subscribedTracksMu.Lock()
	defer c.subscribedTracksMu.Unlock()

	var tracks []*signal_rpc.TrackSubscriptionDetails
	for _, t := range c.subscribedTracks {
		tracks = append(tracks, proto.Clone(t).(*signal_rpc.TrackSubscriptionDetails))
	}
	return tracks
}

func (c *Call) initPubAndSub(options joinOptions) error {
	strategy := options.reconnectDetails.GetStrategy()
	// A fast reconnect keeps the existing peer connections; that is the whole
	// point of it.
	if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST {
		return nil
	}
	// A migration has already installed a fresh peer and is deliberately keeping
	// the old one alive until the SFU confirms the handover, so there is nothing
	// to release here. Every other path replaces the peers outright.
	if strategy != sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE {
		c.releaseOldPubSub(0)
	}

	// A new SFU session knows nothing about this client's subscriptions, so the
	// dedup memo from the previous one no longer describes anything.
	c.subscribedTracksMu.Lock()
	c.sentSubscriptions = nil
	c.subscribedTracksMu.Unlock()

	if c.cc.networkDelay > 0 {
		for _, conf := range []*pc.PeerConfig{&options.subscriberPeerConfig, &options.publisherPeerConfig} {
			delayed, err := netdelay.NewNet(c.cc.networkDelay)
			if err != nil {
				return xerr.Wrap(err)
			}
			conf.SettingEngine.SetNet(delayed)
		}
	}

	sub, err := newSubscriber(c, options.subscriber, options.subscriberPeerConfig, options.beforeSubscriberSendAnswer)
	if err != nil {
		return xerr.Wrap(err)
	}
	pub, err := newPublisher(c, options.publisherPeerConfig)
	if err != nil {
		return xerr.Wrap(err)
	}

	// Swap in a whole new peer rather than mutating the live one field by field:
	// readers go through a single atomic load, so they never see a peer with a
	// new subscriber and a stale publisher.
	current := c.getPeer()
	next := &peer{subscriber: sub, publisher: pub}
	if current != nil {
		next.client.Store(current.client.Load())
	}
	c.peer.Store(next)
	return nil
}

func (c *Call) releaseOldPubSub(sleep time.Duration) {
	sub := c.subscriberPeer()
	if sub != nil {
		sub.Unbind()
	}
	pub := c.publisherPeer()
	if pub != nil {
		pub.Unbind()
	}
	go func() {
		if pub != nil {
			go pub.Close()
		}
		time.Sleep(sleep)
		if sub != nil {
			go sub.Close()
		}
	}()
}

func (c *Call) getPeer() *peer {
	return c.peer.Load()
}

// signalOptions builds the signal client options from the SDK client config, so
// every signal client a call creates -- including the one a migration builds for
// the new SFU -- is configured identically.
func (c *Call) signalOptions() []signal.Option {
	opts := []signal.Option{
		signal.WithLogger(c.logger),
		signal.WithHealthCheck(c.cc.healthCheckInterval, c.cc.healthCheckTimeout),
	}
	if c.statsEnabled() {
		opts = append(opts, signal.WithTracing())
	}
	if c.cc.networkDelay > 0 {
		opts = append(opts, signal.WithDialContext(netdelay.Dialer(c.cc.networkDelay, nil)))
	}
	if c.cc.sfuTransport != nil {
		opts = append(opts, signal.WithRPCTransport(c.cc.sfuTransport))
	}
	return opts
}

// publisherPeer and subscriberPeer return the current peers, or nil when they
// have not been built yet or were torn down by a reconnect. Callers reached from
// the signalling read loop must use these instead of getPeer().publisher so a
// racing teardown cannot panic the process.
func (c *Call) publisherPeer() *publisher {
	p := c.getPeer()
	if p == nil {
		return nil
	}
	return p.publisher
}

func (c *Call) subscriberPeer() *subscriber {
	p := c.getPeer()
	if p == nil {
		return nil
	}
	return p.subscriber
}

// errNoPeerConnection reports that an operation needed a peer connection that
// the call does not currently have.
var errNoPeerConnection = errors.New("no peer connection")

func (c *Call) restoreICE(ctx context.Context) error {
	pub := c.publisherPeer()
	if pub == nil {
		return xerr.Wrap(fmt.Errorf("restore ice: publisher: %w", errNoPeerConnection))
	}
	if err := pub.ICERestart(); err != nil {
		return xerr.Wrap(err)
	}
	resp, err := c.Client().IceRestart(ctx, &signal_rpc.ICERestartRequest{
		SessionId: c.SessionID.Load(),
		PeerType:  sfu_models.PeerType_PEER_TYPE_SUBSCRIBER,
	})
	if err != nil {
		return xerr.Wrap(err)
	}

	if err := resp.GetError(); err != nil {
		return xerr.Wrap(fmt.Errorf("error sending ice restart %s", err))
	}
	return nil
}

func (c *Call) Leave(reason string) error {
	// A fast join returns before its websocket has attached; the leave goes on it.
	if attached := c.attached.Load(); attached != nil {
		select {
		case <-*attached:
		case <-time.After(fastAttachTimeout):
		}
	}
	// Report rtcstats for the last time before leaving
	if err := c.reportRtcStats(c.callCtx); err != nil {
		c.logger.WithField("err", err).Error("failed to report rtcstats to the SFU")
	} else {
		c.logger.Debug("reported rtcstats to the SFU successfully")
	}
	if err := c.Client().SendLeaveCallRequest(c.SessionID.Load(), reason); err != nil {
		return xerr.Wrap(err)
	}
	return c.disconnect()
}

func (c *Call) disconnect() error {
	c.setReconnectStrategyAndDisconnect(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT)
	return nil
}

// RefreshState re-runs the coordinator join to pick up the current call state
// (members, capabilities, settings) and credentials. Join must have succeeded
// first.
func (c *Call) RefreshState(ctx context.Context) error {
	state := c.coordinatorState.Load()
	if state == nil || state.JoinCallRequest == nil {
		return xerr.Error("call has not been joined")
	}

	result, _, err := c.cc.joinCoordinator(ctx, c.Type, c.Id, *state.JoinCallRequest, nil)
	if err != nil {
		return xerr.Wrap(err)
	}

	next := state.clone()
	next.JoinCallRequest = state.JoinCallRequest
	next.CallResponse = result.Call
	next.Members = result.Members
	next.OwnCapabilities = result.OwnCapabilities
	next.StatsOptions = result.StatsOptions
	next.Membership = result.Membership
	c.coordinatorState.Store(&next)
	c.SetCredentials(result.Credentials)
	return nil
}

func (c *Call) RawHandler(event *sfu_events.SfuEvent) {
	c.logger.WithField("event", event.String()).Debug("RawHandler")
}

// OnSubscriberOffer applies an SFU offer to the subscriber peer connection.
//
// The subscriber can legitimately be absent: offers race with the teardown a
// REJOIN or a Leave performs, and this runs on the signalling read loop, so a
// panic here would take the process down with it.
func (c *Call) OnSubscriberOffer(offer *sfu_events.SfuEvent_SubscriberOffer) {
	c.subscriberOffer(time.Now())
	sub := c.subscriberPeer()
	if sub == nil {
		c.logger.Warn("dropping subscriber offer: no subscriber peer connection")
		return
	}
	sub.HandleRemoteDescriptionWithNegotiationID(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offer.SubscriberOffer.Sdp,
	}, offer.SubscriberOffer.NegotiationId)
}

func (c *Call) OnPublisherAnswer(answer *sfu_events.SfuEvent_PublisherAnswer) {
}

func (c *Call) OnConnectionQualityChanged(changed *sfu_events.SfuEvent_ConnectionQualityChanged) {
	c.logger.WithField("quality", changed.ConnectionQualityChanged.String()).Debug("OnConnectionQualityChanged")
}

func (c *Call) OnAudioLevelChanged(changed *sfu_events.SfuEvent_AudioLevelChanged) {
	c.logger.WithField("audio_level", changed.AudioLevelChanged.String()).Debug("OnAudioLevelChanged")
}

func (c *Call) OnIceTrickle(trickle *sfu_events.SfuEvent_IceTrickle) {
	var candidate webrtc.ICECandidateInit
	err := json.Unmarshal([]byte(trickle.IceTrickle.GetIceCandidate()), &candidate)
	if err != nil {
		c.logger.WithField("err", err).Error("failed to unmarshal ice candidate")
		return
	}
	var transport *pc.Transport
	switch trickle.IceTrickle.PeerType {
	case sfu_models.PeerType_PEER_TYPE_SUBSCRIBER:
		if sub := c.subscriberPeer(); sub != nil {
			transport = sub.Transport
		}
	case sfu_models.PeerType_PEER_TYPE_PUBLISHER_UNSPECIFIED:
		if pub := c.publisherPeer(); pub != nil {
			transport = pub.Transport
		}
	default:
		return
	}
	// Trickles race with peer teardown during reconnects; dropping them is
	// correct because the rebuilt peer renegotiates from scratch.
	if transport == nil {
		c.logger.WithField("peer_type", trickle.IceTrickle.PeerType.String()).
			Warn("dropping ice candidate: no peer connection")
		return
	}
	transport.AddICECandidate(candidate)
}

// recordPublishedTrack adds a track to what the call publishes, which is what a
// reconnect restores, and returns its info with the mid it will be sent on.
func (c *Call) recordPublishedTrack(trackInfo *sfu_models.TrackInfo, tracks ...webrtc.TrackLocal) *sfu_models.TrackInfo {
	trackInfo = proto.Clone(trackInfo).(*sfu_models.TrackInfo)
	c.publishedTracksMu.Lock()
	defer c.publishedTracksMu.Unlock()
	// todo, this logic needs to change once we support remove track
	trackInfo.Mid = strconv.Itoa(len(c.publishedTracks))
	c.publishedTracks = append(c.publishedTracks, trackWithInfo{tracks: tracks, info: trackInfo})
	return trackInfo
}

func (c *Call) AddSimulcastTracks(trackInfo *sfu_models.TrackInfo, tracks ...webrtc.TrackLocal) (*webrtc.RTPTransceiver, error) {
	trackInfo = c.recordPublishedTrack(trackInfo, tracks...)
	peerPub := c.publisherPeer()
	if peerPub == nil {
		return nil, xerr.Wrap(fmt.Errorf("add simulcast tracks: %w", errNoPeerConnection))
	}
	transceiver, err := peerPub.AddSimulcastTracks(trackInfo, tracks...)
	if err != nil {
		return nil, err
	}
	peerPub.Negotiate()
	return transceiver, nil
}

func (c *Call) AddTrack(trackInfo *sfu_models.TrackInfo, track webrtc.TrackLocal) (*webrtc.RTPTransceiver, error) {
	trackInfo = c.recordPublishedTrack(trackInfo, track)
	peerPub := c.publisherPeer()
	if peerPub == nil {
		return nil, xerr.Wrap(fmt.Errorf("add track: %w", errNoPeerConnection))
	}
	t, err := peerPub.AddTrack(trackInfo, track)
	if err != nil {
		return nil, err
	}
	peerPub.Negotiate()
	return t, nil
}

func (c *Call) SubscribeToTracks(ctx context.Context, trackDetails ...*signal_rpc.TrackSubscriptionDetails) error {
	c.subscribedTracksMu.Lock()
	// Applications commonly recompute the full subscription set on every
	// participant or layout change and hand back something identical. Sending it
	// again costs an RPC and makes the SFU redo its bookkeeping for nothing.
	unchanged := c.sentSubscriptions != nil && sameSubscriptions(c.sentSubscriptions, trackDetails)
	c.subscribedTracks = cloneSubscriptions(trackDetails)
	c.subscribedTracksMu.Unlock()

	if unchanged {
		c.logger.Debug("skipping updateSubscriptions: subscriptions unchanged")
		return nil
	}
	return c.sendSubscriptions(ctx, trackDetails)
}

// sendSubscriptions tells the SFU what to forward, bypassing the dedup check.
// Callers that have to reach the SFU regardless -- a reconnect restoring state
// onto a session that knows nothing about it -- go through here.
func (c *Call) sendSubscriptions(ctx context.Context, trackDetails []*signal_rpc.TrackSubscriptionDetails) error {
	rec := c.trace.recorder()
	start := time.Now()
	resp, err := c.Client().UpdateSubscriptions(jointrace.WithStep(ctx, rec, jointrace.SubSubscribe, jointrace.PeerSFU),
		&signal_rpc.UpdateSubscriptionsRequest{
			SessionId: c.SessionID.Load(),
			Tracks:    trackDetails,
		})
	if err != nil {
		return xerr.Wrap(err)
	}
	rec.Add(jointrace.Span{
		Name: jointrace.SubSubscribe, After: afterFirst(rec, jointrace.SFUJoin, jointrace.SFUFastJoin),
		Start: start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
	})

	if err := resp.GetError(); err != nil {
		return xerr.Wrap(fmt.Errorf("SubscribeToTracks error: %s", err.String()))
	}

	c.subscribedTracksMu.Lock()
	c.sentSubscriptions = cloneSubscriptions(trackDetails)
	c.subscribedTracksMu.Unlock()
	return nil
}

func cloneSubscriptions(tracks []*signal_rpc.TrackSubscriptionDetails) []*signal_rpc.TrackSubscriptionDetails {
	out := make([]*signal_rpc.TrackSubscriptionDetails, 0, len(tracks))
	for _, t := range tracks {
		out = append(out, proto.Clone(t).(*signal_rpc.TrackSubscriptionDetails))
	}
	return out
}

// sameSubscriptions reports whether two subscription lists request exactly the
// same thing. Order matters only in that the SFU is given a list; comparing
// order-sensitively is deliberate, since a caller that reorders its list is
// cheap to serve and the alternative is sorting on every call.
func sameSubscriptions(current, next []*signal_rpc.TrackSubscriptionDetails) bool {
	if len(current) != len(next) {
		return false
	}
	for i := range current {
		if !proto.Equal(current[i], next[i]) {
			return false
		}
	}
	return true
}

func (c *Call) restorePublishedAndSubscribedTracks() error {
	if err := c.restorePublishedTracks(); err != nil {
		return err
	}
	return c.restoreSubscribedTracks()
}

func (c *Call) restorePublishedTracks() error {
	pub := c.publisherPeer()
	if pub == nil {
		return xerr.Wrap(fmt.Errorf("restore published tracks: %w", errNoPeerConnection))
	}
	c.publishedTracksMu.Lock()
	defer c.publishedTracksMu.Unlock()
	for _, t := range c.publishedTracks {
		if len(t.tracks) == 1 {
			if _, err := pub.AddTrack(t.info, t.tracks[0]); err != nil {
				return err
			}
			continue
		}
		if _, err := pub.AddSimulcastTracks(t.info, t.tracks...); err != nil {
			return err
		}
	}
	// single neg after reestablish
	pub.Negotiate()
	c.logger.Info("restored published tracks, total:", len(c.publishedTracks), "tracks")
	return nil
}

func (c *Call) restoreSubscribedTracks() error {
	c.subscribedTracksMu.Lock()
	tracks := c.subscribedTracks
	c.subscribedTracksMu.Unlock()
	c.logger.Info("restoring subscribed tracks", tracks)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	// Not SubscribeToTracks: the SFU on the other end of a reconnect has no idea
	// what this client was subscribed to, so the dedup check must not apply.
	return c.sendSubscriptions(ctx, tracks)
}

// participantStore returns the call's participant store, or nil before the
// first join has built one. Events can arrive on the websocket before the join
// response is processed, so handlers that touch the store must check.
func (c *Call) participantStore() *ParticipantStore {
	return c.store.Load()
}

func (c *Call) OnParticipantJoined(joined *sfu_events.SfuEvent_ParticipantJoined) {
	store := c.participantStore()
	participant := joined.ParticipantJoined.GetParticipant()
	if store == nil || participant == nil {
		return
	}
	store.Set(NewParticipant(c, participant))
}

func (c *Call) OnParticipantLeft(left *sfu_events.SfuEvent_ParticipantLeft) {
	store := c.participantStore()
	participant := left.ParticipantLeft.GetParticipant()
	if store == nil || participant == nil {
		return
	}
	store.Remove(&Participant{
		ParticipantID: ParticipantID{
			UserID:    UserID(participant.GetUserId()),
			SessionID: SessionID(participant.GetSessionId()),
		},
	})
}

func (c *Call) OnDominantSpeakerChanged(changed *sfu_events.SfuEvent_DominantSpeakerChanged) {
	store := c.participantStore()
	if store == nil {
		return
	}
	newDominantSpeaker := ParticipantID{
		UserID:    UserID(changed.DominantSpeakerChanged.GetUserId()),
		SessionID: SessionID(changed.DominantSpeakerChanged.GetSessionId()),
	}

	store.pByID.Range(func(p *Participant) bool {
		p.IsDominantSpeaker.Store(p.Equal(newDominantSpeaker))
		return true
	})
}

func (c *Call) OnJoinResponse(response *sfu_events.SfuEvent_JoinResponse) {
}

func (c *Call) OnHealthCheckResponse(response *sfu_events.SfuEvent_HealthCheckResponse) {
	store := c.participantStore()
	if store == nil {
		return
	}
	count := response.HealthCheckResponse.GetParticipantCount()
	store.Total.Store(int32(count.GetTotal()))
	store.Anonymous.Store(int32(count.GetAnonymous()))
}

func (c *Call) OnTrackPublished(published *sfu_events.SfuEvent_TrackPublished) {
	store := c.participantStore()
	if store == nil {
		return
	}
	if p := published.TrackPublished.GetParticipant(); p != nil {
		store.Set(NewParticipant(c, p))
		return
	}
	pid := ParticipantID{
		UserID:    UserID(published.TrackPublished.GetUserId()),
		SessionID: SessionID(published.TrackPublished.GetSessionId()),
	}
	p, ok := store.pByID.Load(&Participant{ParticipantID: pid})
	if !ok {
		c.logger.WithField("err", fmt.Errorf("participant not found %#+v", pid)).Error("OnTrackPublished")
		return
	}
	p.PublishedTracks.Set(published.TrackPublished.GetType())
}

// OnInboundStateNotification records which subscribed tracks the SFU has paused.
//
// The SFU pauses a track when nobody is rendering it, saving the bandwidth of
// sending media that would be thrown away. It only does so for clients that
// advertised CLIENT_CAPABILITY_SUBSCRIBER_VIDEO_PAUSE, which this SDK does, so
// without handling this a paused track is indistinguishable from a dead one.
func (c *Call) OnInboundStateNotification(notification *sfu_events.SfuEvent_InboundStateNotification) {
	store := c.store.Load()
	if store == nil {
		return
	}
	var paused []InboundTrackState
	for _, state := range notification.InboundStateNotification.GetInboundVideoStates() {
		pid := ParticipantID{
			UserID:    UserID(state.GetUserId()),
			SessionID: SessionID(state.GetSessionId()),
		}
		if p, ok := store.pByID.Load(&Participant{ParticipantID: pid}); ok {
			p.SetTrackPaused(state.GetTrackType(), state.GetPaused())
		}
		paused = append(paused, InboundTrackState{
			ParticipantID: pid,
			TrackType:     state.GetTrackType(),
			Paused:        state.GetPaused(),
		})
	}

	c.inboundStateMu.Lock()
	handler := c.inboundStateHandler
	c.inboundStateMu.Unlock()
	if handler != nil {
		handler(paused)
	}
}

// InboundTrackState is one entry of an InboundStateNotification: whether the SFU
// is currently forwarding a given participant's track.
type InboundTrackState struct {
	ParticipantID ParticipantID
	TrackType     sfu_models.TrackType
	Paused        bool
}

// OnInboundStateChanged registers a callback fired when the SFU pauses or
// resumes tracks this client subscribes to. Participant state has already been
// updated when it runs. The handler runs on the signalling read loop, so it must
// not block.
func (c *Call) OnInboundStateChanged(handler func([]InboundTrackState)) {
	c.inboundStateMu.Lock()
	defer c.inboundStateMu.Unlock()
	c.inboundStateHandler = handler
}

func (c *Call) OnTrackUnpublished(e *sfu_events.SfuEvent_TrackUnpublished) {
	store := c.participantStore()
	if store == nil {
		return
	}
	if p := e.TrackUnpublished.GetParticipant(); p != nil {
		store.Set(NewParticipant(c, p))
		return
	}
	pid := ParticipantID{
		UserID:    UserID(e.TrackUnpublished.GetUserId()),
		SessionID: SessionID(e.TrackUnpublished.GetSessionId()),
	}
	p, ok := store.pByID.Load(&Participant{ParticipantID: pid})
	if !ok {
		c.logger.WithField("err", fmt.Errorf("participant not found %#+v", pid)).Warn("OnTrackUnpublished")
		return
	}
	p.PublishedTracks.Remove(e.TrackUnpublished.GetType())
}

func (c *Call) OnAudioReceiveSlotBound(bound *sfu_events.SfuEvent_AudioReceiveSlotBound) {
	sub := c.subscriberPeer()
	if sub == nil {
		c.logger.Warn("dropping audio receive slot binding: no subscriber peer connection")
		return
	}
	sub.receiveSlotBound(bound.AudioReceiveSlotBound)
}

func (c *Call) OnError(eventError *sfu_events.SfuEvent_Error) {
	c.logger.WithField("err", eventError.Error.String()).Error("OnError")
	c.setReconnectStrategyAndDisconnect(eventError.Error.GetReconnectStrategy())
}

func (c *Call) OnCallEnded(eventError *sfu_events.SfuEvent_CallEnded) {
	c.logger.WithField("err", eventError.CallEnded.String()).Warn("OnCallEnded")
	err := c.Leave("call ended")
	if err != nil {
		c.logger.WithField("err", err).Error("failed to disconnect")
	}
}

func (c *Call) OnCallGrantsUpdated(updated *sfu_events.SfuEvent_CallGrantsUpdated) {
}

// markDisconnected records the start of an outage, if one is not already in
// progress. The timestamp decides whether a fast reconnect is still in time.
func (c *Call) markDisconnected() {
	c.disconnectedSinceNanos.CompareAndSwap(0, time.Now().UnixNano())
}

// markConnected clears the outage state and replenishes every recovery budget.
func (c *Call) markConnected() {
	c.disconnectedSinceNanos.Store(0)
	c.fastReconnectAttempts.Store(0)
	c.rejoinLimiter.Reset()
}

// disconnectedSince reports when the current outage started, or the zero time
// while connected.
func (c *Call) disconnectedSince() time.Time {
	nanos := c.disconnectedSinceNanos.Load()
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}

// abandonCall gives up on a call that cannot be reconnected, reporting why
// through the unretryable error handler.
func (c *Call) abandonCall(err error) {
	c.logger.WithField("err", err).Error("giving up on the call")
	c.mu.Lock()
	c.nextReconnectStrategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT
	c.mu.Unlock()
	if c.unretryableErrorHandler != nil {
		c.unretryableErrorHandler(err)
	}
}

func (c *Call) monitorHealth() {
	c.connState.Store(CallConnectionStateConnected)
	c.markConnected()
	defer func() {
		c.callCancel()
		c.connState.Store(CallConnectionStateDisconnected)
	}()

	cfg := c.cc.reconnectConfig
	backoff := c.reconnectBackoff

	for {
		// Get the current connection
		conn := c.Client().GetConnection()

		if conn == nil {
			// Connection is nil, attempt to reconnect
			c.logger.Warn("No signal connection, attempting to reconnect")
			c.connState.Store(CallConnectionStateConnecting)
			c.markDisconnected()

			// A call that has been down this long is not coming back on its own.
			if cfg.DisconnectionTimeout > 0 {
				if down := time.Since(c.disconnectedSince()); down > cfg.DisconnectionTimeout {
					c.abandonCall(xerr.Errorf("call disconnected for %s, exceeding the %s budget",
						down.Round(time.Millisecond), cfg.DisconnectionTimeout))
					break
				}
			}

			// Get the current reconnect strategy in a thread-safe manner
			c.mu.Lock()
			strategy := c.nextReconnectStrategy
			c.mu.Unlock()

			if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT {
				c.logger.Warn("Disconnecting for good")
				break
			}

			if strategy < sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST {
				strategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_REJOIN
			}

			// A fast reconnect only works while the SFU still holds the session
			// and both peer connections are intact; otherwise skip straight to a
			// rejoin rather than burning an attempt that cannot succeed.
			strategy = c.selectReconnectStrategy(strategy)

			// Rejoins and migrations are expensive. Rate-limiting them is what
			// stops a permanently broken call from hammering the coordinator.
			if consumesRejoinBudget(strategy) && !c.rejoinLimiter.Allow() {
				c.abandonCall(xerr.Errorf("exceeded %d %s attempts within %s",
					cfg.RejoinLimit, strategy.String(), cfg.RejoinWindow))
				break
			}
			if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST {
				c.fastReconnectAttempts.Add(1)
			}

			err := c.runReconnect(context.Background(), strategy)
			if err != nil {
				if !coordinator.IsRetryableError(err) {
					if c.unretryableErrorHandler != nil {
						c.unretryableErrorHandler(err)
					}
				}

				c.logger.WithField("err", err).Warnf("Failed to reconnect, retrying in %s", backoff)
				time.Sleep(backoff)
				backoff = cfg.nextReconnectBackoff(backoff)
				c.mu.Lock()
				c.nextReconnectStrategy = escalateReconnectStrategy(c.nextReconnectStrategy)
				c.mu.Unlock()
				continue
			}

			// Reconnected successfully
			c.logger.Info("Reconnected successfully")
			c.connState.Store(CallConnectionStateConnected)
			c.markConnected()
			backoff = c.reconnectBackoff
			continue
		}

		c.mu.Lock()
		if c.nextReconnectStrategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT {
			c.mu.Unlock()
			return
		}
		c.nextReconnectStrategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_FAST
		c.mu.Unlock()

		// Connection is not nil, wait for events
		select {
		case <-conn.Liveness():
			// Connection is dead
			c.logger.Warn("Signal connection lost")
			c.connState.Store(CallConnectionStateConnecting)
			// Start the outage clock here rather than waiting for the next
			// iteration, so the fast reconnect deadline is measured from when
			// the connection actually dropped.
			c.markDisconnected()
			continue
		case <-c.migrateRequest:
			// Migration requested
			c.logger.Info("Migration requested")
			c.mu.Lock()
			c.nextReconnectStrategy = sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE
			c.mu.Unlock()
			c.connState.Store(CallConnectionStateConnecting)
			continue
		case <-time.After(c.livenessPoll):
			// No events, continue monitoring
			continue
		}
	}
}

func (c *Call) OnGoAway(away *sfu_events.SfuEvent_GoAway) {
	c.setReconnectStrategyAndDisconnect(sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_MIGRATE)
}

func (c *Call) setReconnectStrategyAndDisconnect(strategy sfu_models.WebsocketReconnectStrategy) {
	c.logger.WithField("strategy", strategy.String()).Warn("setReconnectStrategyAndDisconnect")
	if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_UNSPECIFIED {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextReconnectStrategy = strategy
	if strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT {
		if pub := c.publisherPeer(); pub != nil {
			go pub.Close()
		}
		if sub := c.subscriberPeer(); sub != nil {
			go sub.Close()
		}
	}
	_ = c.Client().Disconnect(strategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT)
}

func (c *Call) OnIceRestart(restart *sfu_events.SfuEvent_IceRestart) {
	c.logger.WithField("restart", restart.IceRestart.String()).Debug("OnIceRestart - publisher ice restart requested")
	pub := c.publisherPeer()
	if pub == nil {
		c.logger.Warn("dropping ice restart request: no publisher peer connection")
		return
	}
	if err := pub.ICERestart(); err != nil {
		c.logger.Println("failed to restart ice", err)
	}
}

func (c *Call) OnPinsUpdated(updated *sfu_events.SfuEvent_PinsUpdated) {
	c.logger.WithField("pins", updated.PinsUpdated.String()).Debug("OnPinsUpdated")
}

func (c *Call) webrtcStatsWorker() {
	defer c.logger.Info("stop stats reporter")
	interval := c.statsInterval()
	if interval == 0 {
		c.logger.Warn("stats reporting is turned off")
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.callCtx.Done():
			return

		case <-ticker.C:
			connState := c.connState.Load()
			if connState == CallConnectionStateDisconnected {
				return
			}
			if connState == CallConnectionStateConnecting {
				c.logger.Debug("skipping stats reporting as the client is still connecting")
				c.statsMetrics.countSkipped()
				continue
			}
			c.statsTick()
		}
	}
}

// statsTick reports one round of stats. The recover is deliberately scoped to a
// single tick: a panic while collecting or encoding stats must not take the
// worker down for the rest of the call.
func (c *Call) statsTick() {
	defer func() {
		if r := recover(); r != nil {
			c.statsMetrics.countFailed()
			c.logger.Errorw("panic while reporting stats", fmt.Errorf("%v", r),
				"stack", string(debug.Stack()))
		}
	}()

	if err := c.report(c.callCtx); err != nil {
		c.logger.WithField("err", err).Error("failed to report stats to the SFU")
	}
	c.logger.Debug("reported stats to the SFU")
	// new rtcstats reporting
	if err := c.reportRtcStats(c.callCtx); err != nil {
		c.logger.WithField("err", err).Error("failed to report rtcstats to the SFU")
	} else {
		c.logger.Debug("reported rtcstats to the SFU successfully")
	}
}

func (c *Call) collectTracing(buf *bytebufferpool.ByteBuffer) {
	samples := make([][]byte, 0, 4)
	if s := c.cc.Tracing.Load().DrainWithComma(false); len(s) > 0 {
		samples = append(samples, s)
	}
	if s := c.Tracing.Load().DrainWithComma(false); len(s) > 0 {
		samples = append(samples, s)
	}
	if pub := c.getPeer().publisher; pub != nil {
		if s := pub.Tracing.Load().DrainWithComma(false); len(s) > 0 {
			samples = append(samples, s)
		}
	}
	if sub := c.getPeer().subscriber; sub != nil {
		if s := sub.Tracing.Load().DrainWithComma(false); len(s) > 0 {
			samples = append(samples, s)
		}
	}

	_, err := buf.Write([]byte("["))
	if err != nil {
		c.logger.WithField("err", err).Error("failed to write tracing object")
		return
	}

	for i, s := range samples {
		if _, err = buf.Write(s); err != nil {
			c.logger.WithField("err", err).Error("failed to write tracing object")
			return
		}
		if i < len(samples)-1 && len(s) > 0 {
			_, err = buf.Write([]byte(","))
			if err != nil {
				c.logger.WithField("err", err).Error("failed to write tracing object")
				return
			}
		}
	}

	_, err = buf.Write([]byte("]"))
	if err != nil {
		c.logger.WithField("err", err).Error("failed to write tracing object")
		return
	}
}

// decodeRtcStats round-trips a getStats map through JSON so delta compression
// sees plain map[string]any records rather than the typed pion structs the
// collectors produce. It returns two independent decodes of the same bytes: one
// to compress and emit, and one to keep as the baseline for the next tick.
// Compression mutates its argument, so the baseline cannot be that same map.
func decodeRtcStats(stats map[string]any) (report, snapshot map[string]any, err error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)

	if err := json.NewEncoder(buf).Encode(stats); err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		return nil, nil, err
	}
	if err := json.Unmarshal(buf.Bytes(), &snapshot); err != nil {
		return nil, nil, err
	}
	return report, snapshot, nil
}

func (c *Call) reportRtcStats(ctx context.Context) error {
	var publisherRtcStats, subscriberRtcStats map[string]any

	// Collect publisher stats
	if pub := c.getPeer().publisher; pub != nil {
		report, snapshot, err := decodeRtcStats(pub.GetRtcStats())
		if err != nil {
			c.statsMetrics.countFailed()
			return xerr.Wrap(err, "failed to encode publisher rtcstats")
		}
		publisherRtcStats = report
		// Evaluate delta compression against the previous tick, then keep this
		// tick's uncompressed snapshot as the next baseline.
		rtcstats.DeltaCompressionRtcStats(c.prevPublisherRtcStats, publisherRtcStats)
		c.prevPublisherRtcStats = snapshot
		// Save event
		pub.Tracing.Load().Emit(rtcstats.GetStatsEvent, publisherRtcStats)
	}

	// Collect subscriber stats
	if sub := c.getPeer().subscriber; sub != nil {
		report, snapshot, err := decodeRtcStats(sub.GetRtcStats())
		if err != nil {
			c.statsMetrics.countFailed()
			return xerr.Wrap(err, "failed to encode subscriber rtcstats")
		}
		subscriberRtcStats = report
		rtcstats.DeltaCompressionRtcStats(c.prevSubscriberRtcStats, subscriberRtcStats)
		c.prevSubscriberRtcStats = snapshot
		// Save event
		sub.Tracing.Load().Emit(rtcstats.GetStatsEvent, subscriberRtcStats)
	}

	statsReportReq := &signal_rpc.SendStatsRequest{
		SessionId:     c.SessionID.Load(),
		WebrtcVersion: WebRTCBuildVersion(),
		Sdk:           humanizeSdkType(sfu_models.SdkType_SDK_TYPE_GO),
		SdkVersion:    c.cc.clientDetails.sdkVersion(),
	}

	rtcstatsBuffer := bytebufferpool.Get()
	defer bytebufferpool.Put(rtcstatsBuffer)
	c.collectTracing(rtcstatsBuffer)
	stats := unsafeString(rtcstatsBuffer.Bytes())
	statsReportReq.RtcStats = stats

	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 3*time.Second)
	resp, err := c.Client().SendStats(ctxWithTimeout, statsReportReq)
	cancelFunc()

	if err != nil {
		c.statsMetrics.countFailed()
		return xerr.Wrap(err, "failed to send rtcstats report to the SFU")
	}
	if respErr := resp.Error; respErr != nil {
		c.statsMetrics.countFailed()
		return xerr.Errorf("sfu reported error %s (message: %s)", respErr.Code.String(), respErr.GetMessage())
	}
	c.statsMetrics.countSuccessful()
	return nil
}

func (c *Call) report(ctx context.Context) error {
	statsReportReq := &signal_rpc.SendStatsRequest{
		SessionId:     c.SessionID.Load(),
		WebrtcVersion: WebRTCBuildVersion(),
		Sdk:           humanizeSdkType(sfu_models.SdkType_SDK_TYPE_GO),
		SdkVersion:    c.cc.clientDetails.sdkVersion(),
	}

	// Why not use the GetStats() API from Pion? Well...It just doesn't work.
	// It gives 0 for all and is mostly USELESS for our purposes.
	publisherStats, subscriberStats := make([]interface{}, 0), make([]interface{}, 0)
	if pub := c.getPeer().publisher; pub != nil {
		publisherStats = pub.GetStats()
	}
	if sub := c.getPeer().subscriber; sub != nil {
		subscriberStats = sub.GetStats()
	}

	// Use bytebufferpool for allocating the buffers for marshaling into JSON.
	// This is because the stats objects are large and are reported a few times
	// every minute creating a lot of garbage. It is easy to avoid.
	pubBuffer, subBuffer := bytebufferpool.Get(), bytebufferpool.Get()
	defer bytebufferpool.Put(pubBuffer)
	defer bytebufferpool.Put(subBuffer)

	pubMarshaler := json.NewEncoder(pubBuffer)
	if err := pubMarshaler.Encode(publisherStats); err != nil {
		c.statsMetrics.countFailed()
		return xerr.Wrap(err, "failed to marshal publisher stats")
	}
	subMarshaler := json.NewEncoder(subBuffer)
	if err := subMarshaler.Encode(subscriberStats); err != nil {
		c.statsMetrics.countFailed()
		return xerr.Wrap(err, "failed to marshal subscriber stats")
	}

	// We don't keep the buffers around once we report. Hence, it is safe and efficient
	// to just access the underlying buffers instead of allocating new byte buffer which
	// is what happens when you do string(someByteBuffer) in go.
	statsReportReq.PublisherStats = unsafeString(pubBuffer.Bytes())
	statsReportReq.SubscriberStats = unsafeString(subBuffer.Bytes())

	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 3*time.Second)
	resp, err := c.Client().SendStats(ctxWithTimeout, statsReportReq)
	cancelFunc()
	if err != nil {
		c.statsMetrics.countFailed()
		return xerr.Wrap(err, "failed to send stats report to the SFU")
	}
	if respErr := resp.Error; respErr != nil {
		c.statsMetrics.countFailed()
		return xerr.Errorf("sfu reported error %s (message: %s)", respErr.Code.String(), respErr.GetMessage())
	}
	c.statsMetrics.countSuccessful()
	return nil
}

func (c *Call) GetStatsReportingMetrics() *CallStatsReportingMetrics {
	return c.statsMetrics.clone()
}

func (c *Call) SendSubscriberRTCP(pkts []rtcp.Packet) error {
	sub := c.subscriberPeer()
	if sub == nil {
		return xerr.Wrap(fmt.Errorf("send subscriber rtcp: %w", errNoPeerConnection))
	}
	return sub.WriteRTCP(pkts)
}

func (c *Call) OnUnretryableError(handler func(error)) {
	c.unretryableErrorHandler = handler
}

func (c *Call) OnPublisherNegotiationFailed(handler func(err *pc.NegotiationError)) {
	c.publisherNegotiationFailedHandler = handler
}

func (c *Call) OnSubscriberNegotiationFailed(handler func(err *pc.NegotiationError)) {
	c.subscriberNegotiationFailedHandler = handler
}

func (c *Call) handlePublisherNegotiationFailed(err *pc.NegotiationError) {
	if c.publisherNegotiationFailedHandler != nil {
		c.publisherNegotiationFailedHandler(err)
	}
}

func (c *Call) handleSubscriberNegotiationFailed(err *pc.NegotiationError) {
	if c.subscriberNegotiationFailedHandler != nil {
		c.subscriberNegotiationFailedHandler(err)
	}
}

// CallStatsReportingMetrics counts the outcome of each stats reporting tick.
// The counters are written from the stats worker goroutine and read by callers
// of GetStatsReportingMetrics, so they are guarded; read a consistent snapshot
// through that method rather than touching the fields of a live instance.
type CallStatsReportingMetrics struct {
	mu                     sync.Mutex
	LastSuccessfulReportAt time.Time
	SuccessfulReports      uint64
	FailedReports          uint64
	SkippedReports         uint64
}

func NewCallStatsReportingMetrics() *CallStatsReportingMetrics {
	return &CallStatsReportingMetrics{}
}

func (c *CallStatsReportingMetrics) countSkipped() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.SkippedReports++
}

func (c *CallStatsReportingMetrics) countFailed() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.FailedReports++
}

func (c *CallStatsReportingMetrics) countSuccessful() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastSuccessfulReportAt = time.Now()
	c.SuccessfulReports++
}

func (c *CallStatsReportingMetrics) clone() *CallStatsReportingMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &CallStatsReportingMetrics{
		LastSuccessfulReportAt: c.LastSuccessfulReportAt,
		SuccessfulReports:      c.SuccessfulReports,
		FailedReports:          c.FailedReports,
		SkippedReports:         c.SkippedReports,
	}
}

// peer bundles everything tied to one SFU connection: the signalling client and
// the two peer connections it negotiated. Grouping them means a migration can
// build a second, independent set and swap it in atomically, instead of mutating
// one set in place while events are still arriving on it.
type peer struct {
	client atomic.Pointer[signal.Client]
	*subscriber
	*publisher
}

func (p *peer) Close() {
	if sub := p.subscriber; sub != nil {
		sub.Close()
	}
	if pub := p.publisher; pub != nil {
		pub.Close()
	}
}

// detach stops this peer from driving the call. Its media keeps flowing and its
// event store keeps recording, but pion callbacks and SFU events no longer reach
// the Call, so a newer peer can own the call's state unopposed.
func (p *peer) detach() {
	if cl := p.client.Load(); cl != nil {
		cl.Detach()
	}
	if sub := p.subscriber; sub != nil {
		sub.Unbind()
	}
	if pub := p.publisher; pub != nil {
		pub.Unbind()
	}
}

// release tears the peer down completely, including its signalling websocket.
func (p *peer) release() {
	p.detach()
	p.Close()
	if cl := p.client.Load(); cl != nil {
		_ = cl.Close()
	}
}

func (p *peer) SetClient(c *signal.Client) {
	p.client.Store(c)
}

func (p *peer) SetSubscriber(s *subscriber) {
	p.subscriber = s
}

func (p *peer) SetPublisher(pub *publisher) {
	p.publisher = pub
}
