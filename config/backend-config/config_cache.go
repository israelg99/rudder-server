//go:generate mockgen -destination=./mock_cacheStore.go -package=backendconfig -source=./config_cache.go cacheStore
package backendconfig

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Cache interface {
	Get(ctx context.Context) (ConfigT, error)
	Set(ctx context.Context, config ConfigT) error
}
type cacheStore struct {
	*sql.DB
	key        string
	workspaces string
}

// returns a new Cache instance, and starts a goroutine to cache the config
func (bc *backendConfigImpl) startCache(ctx context.Context, workspaces string) Cache {
	var (
		err    error
		dbConn *sql.DB
		key    = bc.AccessToken()
	)
	// setup db connection
	dbConn, err = setupDBConn()
	dbStore := cacheStore{
		dbConn,
		key,
		workspaces,
	}
	if err != nil {
		pkgLogger.Errorf("failed to setup db: %v", err)
		return &dbStore
	}
	go func() {
		// subscribe to config and write to db
		ch := bc.Subscribe(ctx, TopicProcessConfig)
		for config := range ch {
			// persist to database
			err = dbStore.Set(ctx, config.Data.(ConfigT))
			if err != nil {
				pkgLogger.Errorf("failed writing config to database: %v", err)
			}
		}
		dbStore.Close()
	}()
	return &dbStore
}

// Encrypt and store the config to the database
func (db *cacheStore) Set(ctx context.Context, config ConfigT) error {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	// write to config table
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO config_cache (workspaces, config) VALUES ($1, pgp_sym_encrypt($2, $3))
		on conflict (workspaces)
		do update set
		config = pgp_sym_encrypt($2, $3),
		updated_at = NOW()`,
		db.workspaces,
		configBytes,
		db.key,
	)
	return err
}

// Fetch the cached config when needed
func (db *cacheStore) Get(ctx context.Context) (ConfigT, error) {
	// read from database
	var (
		config      ConfigT
		configBytes []byte
		err         error
	)
	err = db.QueryRowContext(
		ctx,
		`SELECT pgp_sym_decrypt(config, $1) FROM config_cache WHERE workspaces = $2`,
		db.key,
		db.workspaces,
	).Scan(&configBytes)
	switch err {
	case nil:
	case sql.ErrNoRows:
		// maybe fetch the config where workspaces = ''?
		return config, err
	default:
		return config, err
	}
	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}

// setupDBConn sets up the database connection, creates the config table if it doesn't exist
func setupDBConn() (*sql.DB, error) {
	psqlInfo := jobsdb.GetConnectionString()
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		pkgLogger.Errorf("failed to open db: %v", err)
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		pkgLogger.Errorf("failed to ping db: %v", err)
		return nil, err
	}
	// create table if it doesn't exist
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS config_cache (
		workspaces TEXT NOT NULL PRIMARY KEY,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
		config bytea NOT NULL
	)`)
	if err != nil {
		pkgLogger.Errorf("failed to create table: %v", err)
		return nil, err
	}
	// create encryption extension pgcrypto
	_, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`)
	if err != nil {
		pkgLogger.Errorf("failed to create extension: %v", err)
		return nil, err
	}
	return db, nil
}
