package rtc

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	sfu_events "github.com/GetStream/protocol/protobuf/video/sfu/event"
	sfu_models "github.com/GetStream/protocol/protobuf/video/sfu/models"
	"github.com/GetStream/protocol/protobuf/video/sfu/signal_rpc"
	"github.com/pion/webrtc/v4"
	"github.com/twitchtv/twirp"

	"github.com/GetStream/getstream-go-webrtc/coordinator"
	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/jointrace"
	"github.com/GetStream/getstream-go-webrtc/signal"
)

// JoinFlow is how Join reaches the SFU.
type JoinFlow string

const (
	// JoinFlowFast is the default. The coordinator's fast_join returns candidate SFUs
	// without contacting any; the peer connections and the publisher offer are built
	// while it is in flight. One FastJoin request to the first SFU that takes the client
	// creates the call there if needed, answers the publisher offer and returns the
	// subscriber offer, whose answer goes out without waiting. The SFU websocket
	// attaches alongside and carries only events.
	//
	// A deployment without fast join -- a coordinator or edge that does not know
	// fast_join or has it turned off, SFUs without FastJoin, or an SFU that cannot
	// create the call itself -- is joined with JoinFlowLegacy instead. So is a join that
	// no candidate took in either round of fast_join, unless the refusal holds for the
	// legacy join too, such as a full call. Call.JoinFlow tells which ran, and the join
	// trace's jointrace.FastJoinFallback span why.
	JoinFlowFast JoinFlow = "fast"
	// JoinFlowLegacy is the coordinator's join, which sets the call up on one SFU, then
	// the SFU websocket's JoinRequest, SetPublisher and SendAnswer, each waited for. It
	// is kept for benchmarking the fast join against.
	JoinFlowLegacy JoinFlow = "legacy"
)

// WithJoinFlow picks how Join reaches the SFU. The default is JoinFlowFast.
func WithJoinFlow(flow JoinFlow) JoinOption {
	return func(o *joinOptions) {
		o.flow = flow
	}
}

// WithTrack publishes track from the start of the call, as AddTrack would after Join.
// On the fast join its offer is built while the coordinator request is in flight and
// is answered by the SFU's join itself, so it costs no signalling of its own. A later
// AddTrack costs a renegotiation.
func WithTrack(info *sfu_models.TrackInfo, track webrtc.TrackLocal) JoinOption {
	return func(o *joinOptions) {
		o.tracks = append(o.tracks, trackWithInfo{tracks: []webrtc.TrackLocal{track}, info: info})
	}
}

// JoinFlow is the flow the call's first join took, or "" before it has joined.
func (c *Call) JoinFlow() JoinFlow {
	flow, _ := c.joinFlow.Load().(JoinFlow)
	return flow
}

var (
	// errFastJoinUnavailable means the deployment cannot be fast joined, and the join
	// goes the legacy way.
	errFastJoinUnavailable = errors.New("fast join unavailable")
	// errFastJoinFailed means no candidate of either round took the client, for reasons
	// that may not hold for the legacy join: the join tries it once.
	errFastJoinFailed = errors.New("fast join failed")
)

// fallsBackToLegacy reports whether a failed fast join is followed by a legacy join.
func fallsBackToLegacy(ctx context.Context, err error) bool {
	return ctx.Err() == nil && (errors.Is(err, errFastJoinUnavailable) || errors.Is(err, errFastJoinFailed))
}

// fastJoinFallback records why the join left the fast join for the legacy one, from the
// start of the fast join to that moment.
func fastJoinFallback(rec *jointrace.Recorder, start time.Time, err error) {
	peer := jointrace.PeerSFU
	if coordinator.IsFastJoinUnavailable(err) {
		peer = jointrace.PeerCoordinator
	}
	note := err.Error()
	if len(note) > 200 {
		note = note[:200] + "…"
	}
	rec.Add(jointrace.Span{
		Name: jointrace.FastJoinFallback, Start: start, End: time.Now(),
		Kind: jointrace.KindNet, Peer: peer, Note: note,
	})
}

const (
	// fastJoinRounds is how many times fast_join is asked for candidates when none of
	// them took the client: the grants of the first set may have expired.
	fastJoinRounds = 2
	// fastAttachTimeout bounds the websocket attach. The SFU drops a fast-joined
	// participant whose websocket has not attached after 10 s.
	fastAttachTimeout = 10 * time.Second
	// fastJoinCandidateTimeout bounds the FastJoin to a candidate that has another one
	// after it. It leaves room for a new TLS connection and the SFU creating the call
	// at intercontinental round trips; the last candidate keeps the signal client's
	// own timeout.
	fastJoinCandidateTimeout = 2 * time.Second
)

// fastJoinLocal is what the fast join prepares on the client while fast_join is in
// flight.
type fastJoinLocal struct {
	offer         publisherOffer
	tracks        []*sfu_models.TrackInfo
	subscriberSDP string
}

// fastJoin runs the fast join. When the deployment does not support it, or no candidate
// took the client, it returns an error fallsBackToLegacy accepts, with nothing left behind.
func (c *Call) fastJoin(ctx context.Context, opts []JoinOption, options joinOptions, rec *jointrace.Recorder) (*sfu_events.JoinResponse, error) {
	c.setSessionID(options)
	c.rememberJoinOptions(opts)
	c.externalRTCP = options.externalRTCP
	c.trace.setFast(true, options.publishAfterFastJoin())

	type coordResult struct {
		resp *models.FastJoinCallResponse
		err  error
	}
	coordDone := make(chan coordResult, 1)
	go func() {
		resp, err := c.cc.fastJoinCoordinator(ctx, c.Type, c.Id, options.coordinatorRequest(), rec)
		coordDone <- coordResult{resp, err}
	}()

	local, err := c.prepareFastJoin(ctx, options, rec)
	coord := <-coordDone
	if err == nil {
		err = coord.err
	}
	if err == nil {
		var resp *sfu_events.JoinResponse
		if resp, err = c.fastJoinSFU(ctx, options, local, coord.resp, rec); err == nil {
			if options.publishAfterFastJoin() {
				for _, t := range options.tracks {
					if _, err := c.AddTrack(t.info, t.tracks[0]); err != nil {
						return nil, xerr.Wrap(err)
					}
				}
			}
			return resp, nil
		}
	}

	c.abandonFastJoin(rec)
	if coordinator.IsRefusal(err) && !errors.Is(err, ErrJoinRefused) {
		return nil, fmt.Errorf("%w: %w", ErrJoinRefused, err)
	}
	if coordinator.IsFastJoinUnavailable(err) {
		return nil, fmt.Errorf("%w: the coordinator does not serve fast_join: %w", errFastJoinUnavailable, err)
	}
	return nil, err
}

// prepareFastJoin builds the peer connections, adds the join's tracks and creates the
// publisher offer, and the receive-only SDP the SFU builds the subscriber offer from.
func (c *Call) prepareFastJoin(ctx context.Context, options joinOptions, rec *jointrace.Recorder) (fastJoinLocal, error) {
	var local fastJoinLocal
	start := time.Now()
	if err := c.initPubAndSub(options); err != nil {
		return local, xerr.Wrap(err)
	}
	pub, sub := c.publisherPeer(), c.subscriberPeer()
	pub.fastPath.Store(true)
	sub.fastPath.Store(true)

	sdp, err := subscriberJoinSDPFromMediaEngine(options.subscriberPeerConfig)
	if err != nil {
		return local, xerr.Wrap(err)
	}
	local.subscriberSDP = sdp

	if len(options.tracks) > 0 && !options.publishAfterFastJoin() {
		for _, t := range options.tracks {
			if _, err := pub.AddTrack(c.recordPublishedTrack(t.info, t.tracks...), t.tracks[0]); err != nil {
				return local, xerr.Wrap(err)
			}
		}
		offerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if local.offer, err = pub.joinOfferNow(offerCtx); err != nil {
			return local, err
		}
		local.tracks = pub.trackInfos()
	}
	rec.Add(jointrace.Span{
		Name: jointrace.PCsCreate, Start: start, End: time.Now(),
		Kind: jointrace.KindLocal, Peer: jointrace.PeerLocal,
		Note: "includes the publisher offer, in parallel with coord.fastjoin",
	})
	return local, nil
}

// publishAfterFastJoin reports whether the join's tracks are published after the FastJoin
// instead of in it. A relay-only publisher with no ICE servers of its own would gather
// its offer before it has the SFU's TURN servers, and so with no candidate at all. The
// SFU's TURN accepts its credentials only once the FastJoin has authenticated the token.
func (o joinOptions) publishAfterFastJoin() bool {
	cfg := o.publisherPeerConfig.Config
	return len(o.tracks) > 0 && cfg.ICETransportPolicy == webrtc.ICETransportPolicyRelay && len(cfg.ICEServers) == 0
}

// abandonFastJoin undoes a fast join that did not work out, so a legacy join starts
// from a call that was never joined.
func (c *Call) abandonFastJoin(rec *jointrace.Recorder) {
	c.releaseOldPubSub(0)
	c.publishedTracksMu.Lock()
	c.publishedTracks = nil
	c.publishedTracksMu.Unlock()
	c.trace.setFast(false, false)
	rec.Remove(jointrace.PCsCreate)
}

// fastJoinSFU joins the first candidate SFU that takes the client. If none does, it
// asks the coordinator for new candidates once, sending the SFUs that failed as
// migrating_from_list: fast_join returns other SFUs first, and the same ones, with
// new grants, when there are no others.
func (c *Call) fastJoinSFU(
	ctx context.Context, options joinOptions, local fastJoinLocal, coord *models.FastJoinCallResponse, rec *jointrace.Recorder,
) (*sfu_events.JoinResponse, error) {
	var err error
	var failed []string
	start := time.Now()
	for round := range fastJoinRounds {
		if round > 0 {
			req := options.coordinatorRequest()
			req.MigratingFromList = &failed
			if coord, err = c.cc.fastJoinCoordinator(ctx, c.Type, c.Id, req, nil); err != nil {
				return nil, err
			}
		}
		c.applyFastJoinCoordinator(options, coord)
		var resp *sfu_events.JoinResponse
		resp, err = c.joinCandidates(ctx, options, local, fastJoinRound{n: round, start: start}, coord.Candidates, rec)
		if err == nil || errors.Is(err, errFastJoinUnavailable) || errors.Is(err, ErrJoinRefused) || ctx.Err() != nil {
			return resp, err
		}
		for _, candidate := range coord.Candidates {
			if !slices.Contains(failed, candidate.Server.EdgeName) {
				failed = append(failed, candidate.Server.EdgeName)
			}
		}
		c.logger.WithField("err", err).WithField("failed_sfus", failed).Warn("no fast join candidate took the client")
	}
	return nil, fmt.Errorf("%w: no candidate took the client in %d rounds: %w", errFastJoinFailed, fastJoinRounds, err)
}

// fastJoinRound is which fast_join answer joinCandidates is trying, and when the
// first one's candidates started.
type fastJoinRound struct {
	n     int
	start time.Time
}

// applyFastJoinCoordinator records the call state fast_join returned, as joinCoordinator
// does for join.
func (c *Call) applyFastJoinCoordinator(options joinOptions, resp *models.FastJoinCallResponse) {
	state := &CallState{
		JoinCallRequest: ptrTo(options.coordinatorRequest()),
		CallResponse:    resp.Call,
		Members:         resp.Members,
		OwnCapabilities: resp.OwnCapabilities,
		StatsOptions:    resp.StatsOptions,
		Membership:      resp.Membership,
	}
	if cred := c.cred.Load(); cred != nil {
		state.Url, state.Token, state.WebsocketUrl, state.EdgeName =
			cred.Server.URL, cred.Token, cred.Server.WsEndpoint, cred.Server.EdgeName
	}
	c.coordinatorState.Store(state)
}

// ErrJoinRefused is what a fast Join's error wraps when it stopped at a refusal that
// asking again would not change. An SFU refused the client because the coordinator
// revoked its credentials (the user was kicked or blocked), it may not publish what it
// asked to, or the call is full. Or the coordinator refused the user or their token
// (coordinator.IsRefusal), and the error also wraps that *coordinator.Error. Only a new
// Join, which asks the coordinator for new credentials, can get the user in, if the
// coordinator lets it.
var ErrJoinRefused = errors.New("join refused")

// joinCandidates tries the candidates in order. What happens after a failed one
// follows the SFU's error: see fastJoinOutcome.
func (c *Call) joinCandidates(
	ctx context.Context, options joinOptions, local fastJoinLocal, round fastJoinRound, candidates []models.SFUCandidate, rec *jointrace.Recorder,
) (*sfu_events.JoinResponse, error) {
	if len(candidates) == 0 {
		return nil, xerr.Error("fast_join returned no SFU candidates")
	}
	var errs []error
	unavailable := 0
	start := round.start
	for i, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cred := candidate.Credentials()
		client := signal.NewClient(cred, c, c.signalOptions()...)
		c.getPeer().client.Store(client)
		c.SetCredentials(cred)
		c.startTracing()

		// A candidate's spans are only the join's if it takes the client.
		attempt := rec.Scratch()
		attach := c.dialAttach(client, attempt)
		stepCtx := jointrace.WithStep(ctx, attempt, jointrace.SFUFastJoin, jointrace.PeerSFU)
		req := c.fastJoinRequest(options, local, candidate)
		breakFastJoinGrant(round.n, i, req)
		attemptStart := time.Now()
		resp, err := c.fastJoinCandidate(stepCtx, client, req, i < len(candidates)-1)
		outcome, err := fastJoinOutcome(resp, err)
		if outcome == fastJoinJoined {
			note := ""
			if i > 0 || round.n > 0 {
				note = fmt.Sprintf("candidate %d of %d", i+1, len(candidates))
				if round.n > 0 {
					note += fmt.Sprintf(" of fast_join %d", round.n+1)
				}
				note += fmt.Sprintf(", after %.1f ms on the ones before", float64(attemptStart.Sub(start).Microseconds())/1000)
			}
			attempt.Add(jointrace.Span{
				Name: jointrace.SFUFastJoin, After: []string{jointrace.CoordFastJoin, jointrace.PCsCreate},
				Start: start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerSFU, Note: note,
			})
			serverTimings(stepCtx, resp.GetServerTimings())
			rec.Merge(attempt)
			urls := make([]string, len(candidates))
			for k, offered := range candidates {
				urls[k] = offered.Server.URL
			}
			c.cc.keepSFUsWarm(urls, cred.Server.URL)
			return c.fastJoined(options, local, resp, attach, rec)
		}
		attach.cancel()
		err = xerr.Wrapf(err, "fast join %s", cred.Server.EdgeName)
		c.logger.WithField("err", err).Warn("fast join candidate failed")
		switch outcome {
		case fastJoinLegacy:
			return nil, fmt.Errorf("%w: %w", errFastJoinUnavailable, err)
		case fastJoinRefused:
			return nil, fmt.Errorf("%w: %w", ErrJoinRefused, err)
		case fastJoinUnavailable:
			unavailable++
		}
		errs = append(errs, err)
	}
	if unavailable == len(candidates) {
		return nil, fmt.Errorf("%w: no SFU has FastJoin: %w", errFastJoinUnavailable, errors.Join(errs...))
	}
	return nil, errors.Join(errs...)
}

// fastJoinCandidate sends the FastJoin, giving up after fastJoinCandidateTimeout when
// there is another candidate to try.
func (c *Call) fastJoinCandidate(
	ctx context.Context, client *signal.Client, req *signal_rpc.FastJoinRequest, more bool,
) (*signal_rpc.FastJoinResponse, error) {
	if !more {
		return client.FastJoin(ctx, req)
	}
	ctx, cancel := context.WithTimeout(ctx, fastJoinCandidateTimeout)
	defer cancel()
	return client.FastJoin(ctx, req)
}

func (c *Call) fastJoinRequest(options joinOptions, local fastJoinLocal, candidate models.SFUCandidate) *signal_rpc.FastJoinRequest {
	return &signal_rpc.FastJoinRequest{
		Token:                     candidate.Token,
		SetupGrant:                candidate.SetupGrant,
		SessionId:                 c.SessionID.Load(),
		UnifiedSessionId:          c.unifiedSessionID(),
		PublisherSdp:              local.offer.sdp.SDP,
		Tracks:                    local.tracks,
		SubscriberSdp:             local.subscriberSDP,
		ClientDetails:             c.sfuClientDetails(),
		Capabilities:              options.clientCapabilities(),
		Source:                    c.cc.source.toSfuParticipantSource(),
		PreferredPublishOptions:   options.preferredPublishOptions,
		PreferredSubscribeOptions: options.preferredSubscribeOptions,
		AudioReceiveSlots:         options.audioReceiveSlots,
	}
}

type fastJoinResult int

const (
	fastJoinJoined fastJoinResult = iota
	// fastJoinNext: try the next candidate.
	fastJoinNext
	// fastJoinUnavailable: this SFU has no FastJoin; try the next, and join the legacy
	// way if none has.
	fastJoinUnavailable
	// fastJoinLegacy: the SFU cannot create the call itself; join the legacy way.
	fastJoinLegacy
	// fastJoinRefused: no SFU would take the client; Join fails with ErrJoinRefused.
	fastJoinRefused
)

// fastJoinOutcome classifies a FastJoin answer. The SFU reports its own errors in the
// response with a twirp success; a twirp or transport error is the path to it failing.
func fastJoinOutcome(resp *signal_rpc.FastJoinResponse, err error) (fastJoinResult, error) {
	if err != nil {
		var twirpErr twirp.Error
		if errors.As(err, &twirpErr) && (twirpErr.Code() == twirp.BadRoute || twirpErr.Code() == twirp.Unimplemented) {
			return fastJoinUnavailable, err
		}
		return fastJoinNext, err
	}
	sfuErr := resp.GetError()
	if sfuErr == nil {
		return fastJoinJoined, nil
	}
	err = signal.NewError(sfuErr.GetCode(), sfuErr.GetMessage(), sfuErr.GetShouldRetry())
	if terminalRefusal(sfuErr) {
		return fastJoinRefused, err
	}
	if sfuErr.GetCode() == sfu_models.ErrorCode_ERROR_CODE_INTERNAL_SERVER_ERROR &&
		strings.Contains(sfuErr.GetMessage(), "join through the coordinator") {
		return fastJoinLegacy, err
	}
	// SFU_FULL and SFU_SHUTTING_DOWN created nothing; UNAUTHENTICATED is about this
	// candidate's token or grant, and the next has its own. A grant of a call session
	// that has ended is refused by every candidate, and the next fast_join asks the
	// coordinator, which decides whether the user may join the next session.
	return fastJoinNext, err
}

// terminalRefusal reports whether an SFU refuses the client for a reason that holds on
// every SFU and for the legacy join too, such as a full call: Join fails with it at once.
func terminalRefusal(sfuErr *sfu_models.Error) bool {
	switch sfuErr.GetCode() {
	case sfu_models.ErrorCode_ERROR_CODE_CALL_PARTICIPANT_LIMIT_REACHED:
		return true
	case sfu_models.ErrorCode_ERROR_CODE_PERMISSION_DENIED:
		// The coordinator kicked or blocked the user and revoked the token on every
		// candidate, or the call's other SFUs do not grant the permission either: the
		// other candidates refuse the same way, and a second fast_join would take a
		// kicked user back in without the app asking.
		return true
	}
	return false
}

// serverTimings records the SFU's own account of the FastJoin, as a Server-Timing header
// is recorded for the coordinator.
func serverTimings(ctx context.Context, timings []*signal_rpc.ServerTiming) {
	var total, longest float64
	parts := make([]string, 0, len(timings))
	for _, t := range timings {
		ms := t.GetDurationMs()
		parts = append(parts, t.GetName()+"="+strconv.FormatFloat(ms, 'f', -1, 64))
		if t.GetName() == "total" {
			total = ms
		}
		longest = max(longest, ms)
	}
	if total == 0 {
		total = longest
	}
	jointrace.ServerDuration(ctx, time.Duration(total*float64(time.Millisecond)), "server_timings: "+strings.Join(parts, " "))
}

// fastJoined applies a successful FastJoin: the call state, the publisher answer and
// the subscriber offer. The websocket attach finishes on its own; the health monitor
// starts once it has.
func (c *Call) fastJoined(
	options joinOptions, local fastJoinLocal, resp *signal_rpc.FastJoinResponse, attach *fastAttach, rec *jointrace.Recorder,
) (*sfu_events.JoinResponse, error) {
	now := time.Now()
	c.joinFlow.Store(JoinFlowFast)
	cred := c.credentials()
	// Reconnects and migrations go through the coordinator's join.
	c.GetCred = c.cc.legacyCredentials(c.callCtx, c.Type, c.Id, options.coordinatorRequest(), cred)
	c.cc.watchCall(c.callCtx, c.Type, c.Id)
	c.trace.markJoined()

	joinResp := &sfu_events.JoinResponse{
		CallState:                    resp.GetCallState(),
		FastReconnectDeadlineSeconds: resp.GetFastReconnectDeadlineSeconds(),
		PublishOptions:               resp.GetPublishOptions(),
	}
	// Before the subscriber offer: its tracks are looked up in the store.
	c.store.Store(NewParticipantStore(c, joinResp.GetCallState()))
	c.applyJoinResponse(joinResp)

	pub, sub := c.publisherPeer(), c.subscriberPeer()
	// The peer connections were built before the SFU was chosen. The subscriber gathers
	// when it answers below, so with these servers; an offer in the FastJoin has gathered
	// without them, and the publisher uses them after an ICE restart (but see
	// publishAfterFastJoin). ICE servers the application set stay.
	if len(options.publisherPeerConfig.Config.ICEServers) == 0 {
		setICEServers(pub.PC, cred.IceServers, c)
	}
	if len(options.subscriberPeerConfig.Config.ICEServers) == 0 {
		setICEServers(sub.PC, cred.IceServers, c)
	}
	pub.startTracing()
	sub.startTracing()
	if sdp := resp.GetPublisherSdp(); sdp != "" {
		pub.HandleRemoteDescriptionWithNegotiationID(webrtc.SessionDescription{
			Type: webrtc.SDPTypeAnswer, SDP: sdp,
		}, local.offer.negotiationID)
	}
	if sdp := resp.GetSubscriberSdp(); sdp != "" {
		c.trace.mu.Lock()
		c.trace.subOfferAt = now
		c.trace.mu.Unlock()
		sub.HandleRemoteDescriptionWithNegotiationID(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer, SDP: sdp,
		}, resp.GetSubscriberNegotiationId())
	}

	attached := make(chan struct{})
	c.attached.Store(&attached)
	go func() {
		defer close(attached)
		err := attach.join(c.joinRequestAttach(options), rec)
		if err != nil {
			c.logger.WithField("err", err).Warn("fast join websocket attach failed")
		}
		c.mu.Lock()
		left := c.nextReconnectStrategy == sfu_models.WebsocketReconnectStrategy_WEBSOCKET_RECONNECT_STRATEGY_DISCONNECT
		c.mu.Unlock()
		if left {
			_ = attach.client.Disconnect(true)
			return
		}
		// A failed attach leaves the call without a websocket, which the health
		// monitor repairs with a rejoin.
		c.onceConnect.Do(func() {
			go c.webrtcStatsWorker()
			go c.monitorHealth()
		})
	}()
	return joinResp, nil
}

func (c *Call) joinRequestAttach(options joinOptions) *sfu_events.JoinRequest {
	req := c.joinRequest(options, "", "")
	req.AttachFastJoin = true
	return req
}

func setICEServers(pc *webrtc.PeerConnection, servers []models.ICEServerResponse, c *Call) {
	if len(servers) == 0 {
		return
	}
	cfg := pc.GetConfiguration()
	cfg.ICEServers = iceServers(servers)
	if err := pc.SetConfiguration(cfg); err != nil {
		c.logger.WithField("err", err).Warn("could not set the SFU's ICE servers")
	}
}

// fastAttach is the websocket of a fast join, dialled while the FastJoin is in flight.
// Its JoinRequest waits for the FastJoin to succeed: an attach reaching the SFU before
// the FastJoin finds no participant and fails.
type fastAttach struct {
	client  *signal.Client
	start   time.Time
	dialed  chan struct{}
	conn    *signal.Dialed
	dialErr error
	ctx     context.Context
	stop    context.CancelFunc
	// attempt holds the dial's spans until the candidate has taken the client.
	attempt *jointrace.Recorder
}

func (c *Call) dialAttach(client *signal.Client, attempt *jointrace.Recorder) *fastAttach {
	ctx, stop := context.WithTimeout(c.callCtx, fastAttachTimeout)
	a := &fastAttach{
		client: client, start: time.Now(), dialed: make(chan struct{}), ctx: ctx, stop: stop, attempt: attempt,
	}
	go func() {
		defer close(a.dialed)
		a.conn, a.dialErr = client.Dial(jointrace.WithStep(ctx, attempt, jointrace.SFUWSDial, jointrace.PeerSFU))
		if a.dialErr == nil {
			attempt.Add(jointrace.Span{
				Name: jointrace.SFUWSDial, After: []string{jointrace.CoordFastJoin},
				Start: a.start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
			})
		}
	}()
	return a
}

// cancel drops the websocket of a candidate that did not take the client.
func (a *fastAttach) cancel() {
	a.stop()
	go func() {
		<-a.dialed
		if a.conn != nil {
			a.conn.Close()
		}
	}()
}

// join attaches the websocket to the participant the FastJoin created.
func (a *fastAttach) join(req *sfu_events.JoinRequest, rec *jointrace.Recorder) error {
	defer a.stop()
	<-a.dialed
	rec.Merge(a.attempt)
	if a.dialErr != nil {
		return a.dialErr
	}
	start := time.Now()
	if _, err := a.client.Join(a.ctx, a.conn, req); err != nil {
		return err
	}
	rec.Add(jointrace.Span{
		Name: jointrace.SFUWS, After: []string{jointrace.SFUWSDial, jointrace.SFUFastJoin},
		Start: start, End: time.Now(), Kind: jointrace.KindNet, Peer: jointrace.PeerSFU,
		Note: "attach; nothing waits for it but candidates the FastJoin descriptions lacked",
	})
	return nil
}
