package rtc

import (
	"bytes"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

func TestSetCredentialsDoesNotLogSecrets(t *testing.T) {
	call := newUnconnectedCall(t)

	var out bytes.Buffer
	l := logrus.New()
	l.SetOutput(&out)
	l.SetLevel(logrus.DebugLevel)
	call.logger = logger.FromLogrus(l)

	call.SetCredentials(models.Credentials{
		Token: "sfu-token-must-not-be-logged",
		IceServers: []models.ICEServerResponse{{
			Urls:     []string{"turn:turn.example.com:3478"},
			Username: "turn-user-must-not-be-logged",
			Password: "turn-password-must-not-be-logged",
		}},
		Server: models.SFUResponse{
			EdgeName:   "sfu-edge-1",
			URL:        "https://sfu.example.com/twirp",
			WsEndpoint: "wss://sfu.example.com/ws",
		},
	})

	logged := out.String()
	require.Contains(t, logged, "sfu-edge-1")
	require.NotContains(t, logged, "must-not-be-logged")
}
