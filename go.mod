module github.com/GetStream/getstream-go-webrtc

go 1.27.0

require (
	github.com/GetStream/getstream-go/v5 v5.2.0
	github.com/GetStream/protocol v1.49.0
	github.com/gammazero/deque v1.1.0
	github.com/gobwas/ws v1.4.0
	github.com/golang-jwt/jwt/v5 v5.3.0
	github.com/google/uuid v1.6.0
	github.com/oapi-codegen/runtime v1.6.0
	github.com/pion/dtls/v4 v4.0.0-rc.1
	github.com/pion/ice/v4 v4.4.4
	github.com/pion/interceptor v0.1.49
	github.com/pion/logging v0.2.4
	github.com/pion/rtcp v1.2.18
	github.com/pion/rtp v1.10.5
	github.com/pion/sdp/v3 v3.0.20
	github.com/pion/transport/v5 v5.1.1
	github.com/pion/webrtc/v4 v4.2.22
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.12.1
	github.com/thesyncim/gopus v0.1.1
	github.com/thesyncim/skipset v0.19.0
	github.com/twitchtv/twirp v8.1.3+incompatible
	github.com/valyala/bytebufferpool v1.0.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/gobwas/httphead v0.1.0 // indirect
	github.com/gobwas/pool v0.2.1 // indirect
	github.com/pion/datachannel v1.6.3 // indirect
	github.com/pion/dtls/v3 v3.1.9 // indirect
	github.com/pion/mdns/v2 v2.2.1 // indirect
	github.com/pion/randutil v0.1.0 // indirect
	github.com/pion/sctp v1.11.3 // indirect
	github.com/pion/srtp/v3 v3.1.0 // indirect
	github.com/pion/stun/v4 v4.0.1 // indirect
	github.com/pion/turn/v5 v5.1.2 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.48.0 // indirect
	golang.org/x/net v0.50.0 // indirect
	golang.org/x/sys v0.41.0 // indirect
	golang.org/x/time v0.14.0 // indirect
)

replace github.com/pion/webrtc/v4 => github.com/GetStream/pion-webrtc/v4 v4.2.22-warp.3

replace github.com/pion/ice/v4 => github.com/GetStream/pion-ice/v4 v4.4.4-warp.2

replace github.com/pion/dtls/v4 => github.com/GetStream/pion-dtls/v4 v4.0.0-rc.1-warp.1

replace github.com/pion/sctp => github.com/GetStream/pion-sctp v1.11.3-warp.1
