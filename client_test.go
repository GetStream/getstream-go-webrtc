package rtc

import (
	"testing"
	"time"

	getstream "github.com/GetStream/getstream-go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRTCClient(t *testing.T) {
	t.Parallel()

	t.Run("accepts mixed options", func(t *testing.T) {
		t.Parallel()

		client, err := NewRTCClient("key", "secret",
			WithUser(User{ID: "bot", Name: "Bot"}),
			getstream.WithTimeout(10*time.Second),
			WithoutCoordinatorWS(),
			WithoutLocationDiscovery(),
		)
		require.NoError(t, err)

		assert.Equal(t, "bot", client.UserID)
		assert.Equal(t, "Bot", client.User.Name)
		require.NotNil(t, client.Server())
		assert.Equal(t, 10*time.Second, client.Server().DefaultTimeout())
	})

	t.Run("defaults the user to agent", func(t *testing.T) {
		t.Parallel()

		client, err := NewRTCClient("key", "secret", WithoutCoordinatorWS(), WithoutLocationDiscovery())
		require.NoError(t, err)
		assert.Equal(t, User{ID: "agent", Name: "Agent"}, client.User)
	})

	t.Run("rejects an unknown option", func(t *testing.T) {
		t.Parallel()

		_, err := NewRTCClient("key", "secret", "not an option")
		require.ErrorContains(t, err, "unsupported option string")
	})
}
