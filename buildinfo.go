package rtc

import (
	"runtime/debug"
	"strings"
	"sync"
)

// pionModulePath is the module whose version is reported to the SFU as the
// WebRTC implementation version.
const pionModulePath = "github.com/pion/webrtc/v4"

// sdkModulePath is this module.
const sdkModulePath = "github.com/GetStream/getstream-go-webrtc"

// unknownVersion is reported when the build carries no module information, which
// happens for `go run` and for binaries built with -buildvcs=false.
const unknownVersion = "0.0.0"

var readBuildVersions = sync.OnceValues(func() (sdk string, webrtc string) {
	sdk, webrtc = unknownVersion, unknownVersion
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return sdk, webrtc
	}
	if v := normalizeVersion(info.Main.Version); v != "" && info.Main.Path == sdkModulePath {
		sdk = v
	}
	for _, dep := range info.Deps {
		switch dep.Path {
		case sdkModulePath:
			if v := normalizeVersion(dep.Version); v != "" {
				sdk = v
			}
		case pionModulePath:
			if v := normalizeVersion(dep.Version); v != "" {
				webrtc = v
			}
		}
	}
	return sdk, webrtc
})

// SDKBuildVersion reports this SDK's module version, as recorded in the binary's
// build info. It falls back to "0.0.0" when the build has none.
func SDKBuildVersion() string {
	sdk, _ := readBuildVersions()
	return sdk
}

// WebRTCBuildVersion reports the version of the pion/webrtc module this binary
// was built against, so the SFU's telemetry attributes behaviour to the right
// stack instead of a hardcoded placeholder.
func WebRTCBuildVersion() string {
	_, webrtc := readBuildVersions()
	return webrtc
}

// normalizeVersion turns a module version into the bare semver the SFU expects,
// or "" if there is nothing usable. Development builds report "(devel)", and
// replaced or vendored modules can report an empty version.
func normalizeVersion(v string) string {
	if v == "" || v == "(devel)" {
		return ""
	}
	return strings.TrimPrefix(v, "v")
}

// sdkVersion returns the version to report for this client: the explicitly
// configured one when the application set it, otherwise the build's.
func (c ClientDetails) sdkVersion() string {
	if v := c.SDKVersion.string(); v != unknownVersion {
		return v
	}
	return SDKBuildVersion()
}

func (v SDKVersion) string() string {
	if v.Major == "" && v.Minor == "" && v.Patch == "" {
		return unknownVersion
	}
	return v.Major + "." + v.Minor + "." + v.Patch
}
