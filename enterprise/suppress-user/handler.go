package suppression

import "github.com/rudderlabs/rudder-server/utils/logger"

// handler is a handle to this object
type handler struct {
	log logger.Logger
	r   Repository
}

func (h *handler) IsSuppressedUser(workspaceID, userID, sourceID string) bool {
	h.log.Debugf("IsSuppressedUser called for workspace: %s, user %s, source %s", workspaceID, sourceID, userID)
	suppressed, err := h.r.Suppress(userID, workspaceID, sourceID)
	if err != nil {
		h.log.Errorf("Suppression check failed for user: %s, workspace: %s, source: %s: %w", userID, workspaceID, sourceID, err)
		return false
	}
	return suppressed
}
