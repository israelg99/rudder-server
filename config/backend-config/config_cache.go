package backendconfig

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

var (
	db *sql.DB
)

// new goroutine here to cache the config
func cache(ctx context.Context, workspaces string) {
	var err error
	// setup db connection
	db, err = setupDBConn()
	if err != nil {
		pkgLogger.Errorf("failed to setup db: %v", err)
		return
	}
	defer db.Close()

	// subscribe to config and write to db
	ch := backendConfig.Subscribe(ctx, TopicProcessConfig)
	for config := range ch {
		// persist to database
		configBytes, err := json.Marshal(config)
		if err != nil {
			pkgLogger.Errorf("failed to marshal config: %v", err)
		}
		err = persistConfig(ctx, configBytes, db, workspaces)
		if err != nil {
			pkgLogger.Errorf("failed writing config to database: %v", err)
		}
	}
}

// Encrypt and store the config to the database
func persistConfig(ctx context.Context, configBytes []byte, db *sql.DB, workspaces string) error {
	// write to config table
	_, err := db.ExecContext(
		ctx,
		`INSERT INTO config_cache (workspaces, config) VALUES ($1, pgp_sym_encrypt($2, $3))`,
		workspaces,
		configBytes,
		DefaultBackendConfig.AccessToken(),
	)
	return err
}

// Fetch the cached config when needed
func getCachedConfig(ctx context.Context, workspaces string) (ConfigT, error) {
	// read from database
	var config ConfigT
	var configBytes []byte
	err := db.QueryRowContext(
		ctx,
		`SELECT pgp_sym_decrypt(config, $1) FROM config_cache WHERE workspaces = $2 order by created_at desc limit 1`,
		DefaultBackendConfig.AccessToken(),
		workspaces,
	).Scan(&configBytes)
	if err != nil {
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
		id SERIAL PRIMARY KEY,
		workspaces TEXT NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
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
