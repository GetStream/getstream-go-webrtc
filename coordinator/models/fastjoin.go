package models

// FastJoinCallResponse is the coordinator's answer to fast_join: the call state join
// returns, plus the SFUs the client may join, in the order to try them. The coordinator
// makes no call to any SFU: the first SFU the client reaches creates the call from the
// candidate's setup grant.
type FastJoinCallResponse struct {
	Call            CallResponse     `json:"call"`
	Created         bool             `json:"created"`
	Duration        string           `json:"duration"`
	Members         []MemberResponse `json:"members"`
	Membership      *MemberResponse  `json:"membership,omitempty"`
	OwnCapabilities []OwnCapability  `json:"own_capabilities"`
	StatsOptions    StatsOptions     `json:"stats_options"`
	Candidates      []SFUCandidate   `json:"candidates"`
}

// SFUCandidate is one SFU a fast join may go to. Its token is bound to that SFU, so a
// candidate's token, ICE servers and grant are only ever used with its own server.
type SFUCandidate struct {
	Server     SFUResponse         `json:"server"`
	Token      string              `json:"token"`
	IceServers []ICEServerResponse `json:"ice_servers"`
	// SetupGrant is the coordinator-signed call setup the SFU verifies to create the call.
	SetupGrant string `json:"setup_grant"`
}

// Credentials returns the candidate as the credentials the rest of the SDK takes.
func (c SFUCandidate) Credentials() Credentials {
	return Credentials{IceServers: c.IceServers, Server: c.Server, Token: c.Token}
}
