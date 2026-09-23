package rtc

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/GetStream/getstream-go-webrtc/internal/xerr"
	"github.com/GetStream/getstream-go-webrtc/logger"
)

const (
	// HeaderCloudFrontPop is the CloudFront response header carrying the edge
	// point of presence that served the request. Its first three characters are
	// the airport code Stream uses as a location hint.
	HeaderCloudFrontPop = "X-Amz-Cf-Pop"

	// FallbackLocationName is the hint used when discovery fails.
	FallbackLocationName = "IAD"

	// DefaultLocationHintURL is the endpoint probed to discover the nearest
	// CloudFront POP. Override it with WithLocationHintURL.
	DefaultLocationHintURL = "https://hint.stream-io-video.com/"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type LocationDiscovery interface {
	Discover(_ context.Context) string
}

type CloudFrontDiscovery struct {
	client     HTTPClient
	url        string
	maxRetries int
	logger     logger.ILogger
}

var locationHTTPClient *http.Client

func init() {
	locationHTTPClient = &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          10,
			IdleConnTimeout:       10 * time.Second,
			TLSHandshakeTimeout:   1 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: time.Second,
	}
}

func NewCloudFrontDiscovery(url string, maxRetries int, client HTTPClient, log logger.ILogger) *CloudFrontDiscovery {
	if url == "" {
		url = DefaultLocationHintURL
	}
	if log == nil {
		log = logger.Noop{}
	}
	return &CloudFrontDiscovery{
		url:        url,
		maxRetries: maxRetries,
		client:     client,
		logger:     log,
	}
}

func (c *CloudFrontDiscovery) Discover(ctx context.Context) string {
	r, err := http.NewRequestWithContext(ctx, http.MethodHead, c.url, http.NoBody)
	if err != nil {
		c.logger.Warn(xerr.Wrap(err))
		return FallbackLocationName
	}

	for i := 0; i < c.maxRetries; i++ {
		c.logger.Infof("Discovering location, attempt %d", i+1)
		resp, err := c.client.Do(r)
		if err != nil {
			c.logger.Warn(xerr.Wrapf(err, "HEAD request failed"))
			continue
		}
		if resp.StatusCode != http.StatusOK {
			c.logger.Warn(xerr.Errorf("unexpected status code: %d", resp.StatusCode))
			continue
		}
		popName := []rune(resp.Header.Get(HeaderCloudFrontPop))
		_ = resp.Body.Close()

		if len(popName) < 3 {
			c.logger.Warn(xerr.Errorf("invalid pop name: %q", string(popName)))
			return FallbackLocationName
		}

		return string(popName[:3])
	}

	return FallbackLocationName
}
