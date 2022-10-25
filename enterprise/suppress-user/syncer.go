package suppression

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// SyncerOpt represents a configuration option for the syncer
type SyncerOpt func(*Syncer)

// WithHttpClient sets the http client to use
func WithHttpClient(client *http.Client) SyncerOpt {
	return func(c *Syncer) {
		c.client = client
	}
}

// WithPageSize sets the page size for each sync request
func WithPageSize(pageSize int) SyncerOpt {
	return func(c *Syncer) {
		c.pageSize = pageSize
	}
}

// WithPollIntervalFn sets the interval at which the syncer will poll the backend
func WithPollIntervalFn(pollIntervalFn func() time.Duration) SyncerOpt {
	return func(c *Syncer) {
		c.pollIntervalFn = pollIntervalFn
	}
}

// WithLogger sets the logger to use in the syncer
func WithLogger(log logger.Logger) SyncerOpt {
	return func(c *Syncer) {
		c.log = log
	}
}

// NewSyncer creates a new syncer
func NewSyncer(baseURL string, identifier identity.Identifier, r Repository, opts ...SyncerOpt) *Syncer {
	s := &Syncer{
		baseURL:            baseURL,
		identifier:         identifier,
		r:                  r,
		log:                logger.NOP,
		client:             &http.Client{},
		pageSize:           100,
		pollIntervalFn:     func() time.Duration { return 5 * time.Minute },
		defaultWorkspaceID: identifier.ID(),
	}
	for _, opt := range opts {
		opt(s)
	}
	var err error
	if s.state.token, err = r.GetToken(); err != nil {
		s.log.Errorf("Failed to get token from repository: %w", err)
	}

	return s
}

// Syncer is responsible for syncing suppressions from the backend to the repository
type Syncer struct {
	baseURL    string
	identifier identity.Identifier
	r          Repository

	client             *http.Client
	log                logger.Logger
	pageSize           int
	pollIntervalFn     func() time.Duration
	defaultWorkspaceID string

	state struct {
		token []byte
	}
}

// SyncLoop runs the sync loop until the provided context is done
func (s *Syncer) SyncLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.pollIntervalFn()):
		}
	again:
		s.log.Info("Fetching Regulations")
		suppressions, nextToken, err := s.sync()
		if err != nil {
			continue
		}
		// TODO: this won't be needed once data regulation service gets updated
		for i := range suppressions {
			suppression := &suppressions[i]
			if suppression.WorkspaceID == "" {
				suppression.WorkspaceID = s.defaultWorkspaceID
			}
		}
		err = s.r.Add(suppressions, s.state.token)
		if err != nil {
			s.log.Errorf("Failed to add %d suppressions to repository: %w", len(suppressions), err)
			continue
		}
		s.state.token = nextToken
		if len(suppressions) == s.pageSize {
			goto again
		}
	}
}

// sync fetches suppressions from the backend and adds them to the repository
func (s *Syncer) sync() ([]model.Suppression, []byte, error) {
	urlStr := fmt.Sprintf("%s/dataplane/workspaces/%s/regulations/suppressions", s.baseURL, s.identifier.ID())
	urlValQuery := url.Values{}
	if s.pageSize > 0 {
		urlValQuery.Set("pageSize", strconv.Itoa(s.pageSize))
	}
	if len(s.state.token) > 0 {
		urlValQuery.Set("pageToken", string(s.state.token))
	}
	if len(urlValQuery) > 0 {
		urlStr += "?" + urlValQuery.Encode()
	}

	var resp *http.Response
	var respBody []byte

	operation := func() error {
		var err error
		req, err := http.NewRequest("GET", urlStr, http.NoBody)
		s.log.Debugf("regulation service URL: %s", urlStr)
		if err != nil {
			return err
		}
		workspaceToken := config.GetWorkspaceToken()
		req.SetBasicAuth(workspaceToken, "")
		req.Header.Set("Content-Type", "application/json")

		resp, err = s.client.Do(req)
		if err != nil {
			return err
		}
		defer func() {
			err := resp.Body.Close()
			if err != nil {
				s.log.Error(err)
			}
		}()

		// If statusCode is not 2xx, then returning empty regulations
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = fmt.Errorf("status code %v", resp.StatusCode)
			s.log.Errorf("[[ Workspace-config ]] Failed to fetch source regulations. statusCode: %v, error: %v",
				resp.StatusCode, err)
			return err
		}

		respBody, err = io.ReadAll(resp.Body)
		if err != nil {
			s.log.Error(err)
			return err
		}
		return err
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		s.log.Errorf("[[ Workspace-config ]] Failed to fetch source regulations from API with error: %v, retrying after %v", err, t)
	})
	if err != nil {
		s.log.Error("Error sending request to the server: ", err)
		return []model.Suppression{}, nil, err
	}
	if respBody == nil {
		s.log.Error("nil response body, returning")
		return []model.Suppression{}, nil, errors.New("nil response body")
	}
	var respJSON suppressionsResponse
	err = json.Unmarshal(respBody, &respJSON)
	if err != nil {
		s.log.Error("Error while parsing response: ", err, resp.StatusCode)
		return []model.Suppression{}, nil, err
	}

	if respJSON.Token == "" {
		s.log.Errorf("[[ Workspace-config ]] No token found in the source regulations response: %v", string(respBody))
		return respJSON.Items, nil, fmt.Errorf("no token returned in regulation API response")
	}
	return respJSON.Items, []byte(respJSON.Token), nil
}

type suppressionsResponse struct {
	Items []model.Suppression `json:"items"`
	Token string              `json:"token"`
}
