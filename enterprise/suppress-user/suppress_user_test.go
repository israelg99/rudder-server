package suppression

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var _ = Describe("suppress user test", func() {
	type syncResponse struct {
		statusCode int
		respBody   []byte
	}
	identifier := &identity.Workspace{
		WorkspaceID: "workspace-1",
	}
	defaultSuppression := model.Suppression{
		Canceled:    false,
		WorkspaceID: "workspace-1",
		UserID:      "user-1",
		SourceIDs:   []string{"src-1", "src-2"},
	}
	defaultResponse := suppressionsResponse{
		Items: []model.Suppression{defaultSuppression},
		Token: "tempToken123",
	}

	var h *handler
	var serverResponse syncResponse
	var server *httptest.Server
	newTestServer := func() *httptest.Server {
		var count int
		var prevRespBody []byte
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var respBody []byte
			// send the expected payload if it is the first time or the payload has changed
			if count == 0 || prevRespBody != nil && string(prevRespBody) != string(serverResponse.respBody) {
				respBody = serverResponse.respBody
				prevRespBody = serverResponse.respBody
				count++
			} else { // otherwise send an response containing no items
				respBody, _ = json.Marshal(suppressionsResponse{
					Token: "tempToken123",
				})
			}

			w.WriteHeader(serverResponse.statusCode)
			_, _ = w.Write(respBody)
		}))
	}

	BeforeEach(func() {
		config.Reset()
		backendconfig.Init()
		server = newTestServer()
		h = &handler{
			log: logger.NOP,
			r:   NewMemoryRepository(logger.NOP),
		}
	})
	AfterEach(func() {
		server.Close()
	})
	Context("sync error scenarios", func() {
		It("returns an error when a wrong server address is provided", func() {
			_, _, err := NewSyncer("", identifier, h.r).sync()
			Expect(err.Error()).NotTo(Equal(nil))
		})

		It("returns an error when server responds with HTTP 500", func() {
			serverResponse = syncResponse{
				statusCode: 500,
				respBody:   []byte(""),
			}
			_, _, err := NewSyncer(server.URL, identifier, h.r).sync()
			Expect(err.Error()).To(Equal("status code 500"))
		})

		It("returns an error when server responds with invalid (empty) data in the response body", func() {
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   []byte(""),
			}
			_, _, err := NewSyncer(server.URL, identifier, h.r).sync()
			Expect(err.Error()).To(Equal("unexpected end of JSON input"))
		})

		It("returns an error when server responds with invalid (corrupted json) data in the response body", func() {
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   []byte("{w"),
			}
			_, _, err := NewSyncer(server.URL, identifier, h.r).sync()
			Expect(err.Error()).To(Equal("invalid character 'w' looking for beginning of object key string"))
		})

		It("returns an error when server responds with no token in the response body", func() {
			resp := defaultResponse
			resp.Token = ""
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			suppressions, _, err := NewSyncer(server.URL, identifier, h.r).sync()
			Expect(err.Error()).To(Equal("no token returned in regulation API response"))
			Expect(suppressions).To(Equal(defaultResponse.Items))
		})
	})

	Context("handler, repository, syncer integration", func() {
		It("exact user suppression match", func() {
			respBody, _ := json.Marshal(defaultResponse)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := NewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeTrue())
		})

		It("user suppression added, then cancelled", func() {
			resp := defaultResponse
			resp.Items = []model.Suppression{
				defaultSuppression,
				{
					Canceled:  false,
					UserID:    "user-2",
					SourceIDs: []string{"src-1", "src-2"},
				},
			}
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := NewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-2", "src-1") }).Should(BeTrue())
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeTrue())

			resp.Items[0].Canceled = true
			respBody, _ = json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeFalse())
		})

		It("wildcard user suppression match", func() {
			resp := defaultResponse
			resp.Items = []model.Suppression{
				{
					Canceled:  false,
					UserID:    "user-1",
					SourceIDs: []string{},
				},
				{
					Canceled:  false,
					UserID:    "user-2",
					SourceIDs: []string{"src-2"},
				},
			}
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := NewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeTrue())
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-2", "src-2") }).Should(BeTrue())
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-2", "src-1") }).Should(BeFalse())
		})

		It("wildcard user suppression rule added and then cancelled", func() {
			resp := defaultResponse
			resp.Items = []model.Suppression{
				{
					Canceled:  false,
					UserID:    "user-1",
					SourceIDs: []string{},
				},
				{
					Canceled:  false,
					UserID:    "user-2",
					SourceIDs: []string{"src-2"},
				},
			}
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := NewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()

			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeTrue())
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-2", "src-2") }).Should(BeTrue())
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-2", "src-1") }).Should(BeFalse())

			resp.Items[0].Canceled = true
			respBody, _ = json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			Eventually(func() bool { return h.IsSuppressedUser("workspace-1", "user-1", "src-1") }).Should(BeFalse())
		})
	})
})
