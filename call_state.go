package rtc

import (
	"slices"

	"github.com/GetStream/getstream-go-webrtc/coordinator/models"
)

type CallState struct {
	models.CallResponse
	EdgeName        string                  `json:"edge_name"`
	Url             string                  `json:"url"`
	Token           string                  `json:"token"`
	WebsocketUrl    string                  `json:"websocket_url"`
	BlockedUsers    []models.UserResponse   `json:"blocked_users"`
	Members         []models.MemberResponse `json:"members"`
	OwnCapabilities []models.OwnCapability  `json:"own_capabilities"`
	StatsOptions    models.StatsOptions     `json:"stats_options"`
	Membership      *models.MemberResponse  `json:"membership"`
	JoinCallRequest *models.JoinCallRequest
}

func (c CallState) clone() CallState {
	return CallState{
		EdgeName:        c.EdgeName,
		Url:             c.Url,
		Token:           c.Token,
		WebsocketUrl:    c.WebsocketUrl,
		CallResponse:    c.CallResponse,
		BlockedUsers:    slices.Clone(c.BlockedUsers),
		Members:         slices.Clone(c.Members),
		OwnCapabilities: slices.Clone(c.OwnCapabilities),
		StatsOptions:    c.StatsOptions,
		Membership:      c.Membership,
	}
}
