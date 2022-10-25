package suppression

import (
	"fmt"
	"io"
	"sync"

	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/badgerdb"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/internal/memory"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	ErrRestoring    = model.ErrRestoring
	ErrNotSupported = model.ErrNotSupported
)

// Repository provides a generic interface for managing user suppressions
type Repository interface {
	// Stop stops the repository
	Stop() error

	// GetToken returns the current token
	GetToken() ([]byte, error)

	// Add adds the given suppressions to the repository
	Add(suppressions []model.Suppression, token []byte) error

	// Suppress returns true if the given user is suppressed, false otherwise
	Suppress(userID, workspaceID, sourceID string) (bool, error)

	// Backup writes a backup of the repository to the given writer
	Backup(w io.Writer) error

	// Restore restores the repository from the given reader
	Restore(r io.Reader) error
}

// NewMemoryRepository returns a new repository backed by memory.
func NewMemoryRepository(log logger.Logger) Repository {
	return memory.NewRepository(log)
}

var (
	WithSeederSource = badgerdb.WithSeederSource
	WithMaxSeedWait  = badgerdb.WithMaxSeedWait
)

// NewBadgerRepository returns a new repository backed by badgerDB.
func NewBadgerRepository(path string, log logger.Logger, opts ...badgerdb.Opt) (Repository, error) {
	return badgerdb.NewRepository(path, log, opts...)
}

type RestoreSafeRepository struct {
	restoringLock sync.RWMutex
	restoring     bool
	Repository
}

func (r *RestoreSafeRepository) Restore(reader io.Reader) error {
	r.restoringLock.Lock()
	r.restoring = true
	r.restoringLock.Unlock()
	return r.Repository.Restore(reader)
}

func (r *RestoreSafeRepository) GetToken() ([]byte, error) {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return nil, fmt.Errorf("repository is restoring")
	}

	return r.Repository.GetToken()
}

func (r *RestoreSafeRepository) Add(suppressions []model.Suppression, token []byte) error {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return fmt.Errorf("repository is restoring")
	}
	return r.Repository.Add(suppressions, token)
}

func (r *RestoreSafeRepository) Suppress(userID, workspaceID, sourceID string) (bool, error) {
	r.restoringLock.RLock()
	defer r.restoringLock.RUnlock()
	if r.restoring {
		return false, fmt.Errorf("repository is restoring")
	}
	return r.Repository.Suppress(userID, workspaceID, sourceID)
}
