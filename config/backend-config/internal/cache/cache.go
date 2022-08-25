//go:generate mockgen -destination=./mock_cache.go -package=cache -source=./cache.go cache
package cache

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/rudderlabs/rudder-server/config"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
)

var pkgLogger = logger.NewLogger().Child("backend-config-cache")

type Cache interface {
	Get(ctx context.Context) ([]byte, error)
}

type cacheStore struct {
	*sql.DB
	secret string
	key    string
}

// Start returns a new Cache instance, and starts a goroutine to cache the config
//
// secret is the secret key to encrypt the config with before storing it
//
// key is the key to use to store and fetch the config from the cache store
//
// ch is the channel to listen on for config updates and store them
func Start(ctx context.Context, secret, key string, ch pubsub.DataChannel) Cache {
	var (
		err    error
		dbConn *sql.DB
	)
	// setup db connection
	dbConn, err = setupDBConn()
	dbStore := cacheStore{
		dbConn,
		secret,
		key,
	}
	if err != nil {
		pkgLogger.Errorf("failed to setup db: %v", err)
		return &dbStore
	}

	// apply migrations
	err = migrate(dbConn)
	if err != nil {
		pkgLogger.Errorf("failed to migrate db: %v", err)
		return &dbStore
	}

	// clear config for other keys
	err = dbStore.clear(ctx)
	if err != nil {
		pkgLogger.Errorf("failed to clear previous config: %v", err)
		return &dbStore
	}

	go func() {
		// subscribe to config and write to db
		for config := range ch {
			// persist to database
			err = dbStore.set(ctx, config.Data)
			if err != nil {
				pkgLogger.Errorf("failed writing config to database: %v", err)
			}
		}
		dbStore.Close()
	}()
	return &dbStore
}

// Encrypt and store the config to the database
func (db *cacheStore) set(ctx context.Context, config interface{}) error {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	// write to config table
	_, err = db.ExecContext(
		ctx,
		`INSERT INTO config_cache (key, config) VALUES ($1, pgp_sym_encrypt($2, $3))
		on conflict (key)
		do update set
		config = pgp_sym_encrypt($2, $3),
		updated_at = NOW()`,
		db.key,
		configBytes,
		db.secret,
	)
	return err
}

// Fetch the cached config when needed
func (db *cacheStore) Get(ctx context.Context) ([]byte, error) {
	// read from database
	var (
		config []byte
		err    error
	)
	err = db.QueryRowContext(
		ctx,
		`SELECT pgp_sym_decrypt(config, $1) FROM config_cache WHERE key = $2`,
		db.secret,
		db.key,
	).Scan(&config)
	switch err {
	case nil:
	case sql.ErrNoRows:
		// maybe fetch the config where workspaces = ''?
		return config, err
	default:
		return config, err
	}
	return config, nil
}

// setupDBConn sets up the database connection, creates the config table if it doesn't exist
func setupDBConn() (*sql.DB, error) {
	psqlInfo := misc.GetConnectionString()
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
	// // create table if it doesn't exist
	// _, err = db.Exec(`CREATE TABLE IF NOT EXISTS config_cache (
	// 	key TEXT NOT NULL PRIMARY KEY,
	// 	created_at TIMESTAMP NOT NULL DEFAULT NOW(),
	// 	updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
	// 	config bytea NOT NULL
	// )`)
	// if err != nil {
	// 	pkgLogger.Errorf("failed to create table: %v", err)
	// 	return nil, err
	// }
	// // create encryption extension pgcrypto
	// _, err = db.Exec(`CREATE EXTENSION IF NOT EXISTS "pgcrypto"`)
	// if err != nil {
	// 	pkgLogger.Errorf("failed to create extension: %v", err)
	// 	return nil, err
	// }
	return db, nil
}

// clear config for all other keys
func (db *cacheStore) clear(ctx context.Context) error {
	_, err := db.ExecContext(ctx, `DELETE FROM config_cache WHERE key != $1`, db.key)
	return err
}

// apply config_cache migrations to the database
func migrate(db *sql.DB) error {
	m := &migrator.Migrator{
		Handle:                     db,
		MigrationsTable:            "config_cache_migrations",
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", true),
	}

	return m.Migrate("config_cache")

}
