// Copyright (c) Roman Atachiants and contributors. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"time"

	"github.com/kelindar/trend"
	_ "github.com/ncruces/go-sqlite3/driver" // cgo-free, uses wazero
)

type store struct {
	db     *sql.DB
	prefix string
	own    bool
}

func init() {
	trend.Register("sqlite", Open)
	trend.Register("sqlite3", Open)
}

func Open(u *url.URL) (trend.Store, error) {
	path := sqlitePath(u)
	if path == "" {
		path = ":memory:"
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &store{db: db, prefix: u.Query().Get("prefix"), own: true}, nil
}

func New(db *sql.DB, prefix string) (trend.Store, error) {
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		return nil, err
	}
	return &store{db: db, prefix: prefix}, nil
}

func (s *store) Load(ctx context.Context, key string) ([]byte, error) {
	var out []byte
	err := s.db.QueryRowContext(ctx, `SELECT value FROM trend WHERE key = ?`, s.prefix+key).Scan(&out)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return out, err
}

func (s *store) Update(ctx context.Context, key string, merge func([]byte) ([]byte, error)) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	key = s.prefix + key
	var old []byte
	err = tx.QueryRowContext(ctx, `SELECT value FROM trend WHERE key = ?`, key).Scan(&old)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	next, err := merge(old)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO trend(key, value) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, next); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) Delete(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM trend WHERE key = ?`, s.prefix+key)
	return err
}

func (s *store) Lease(ctx context.Context, key string, ttl time.Duration) (func(context.Context) error, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback() }()

	key = s.prefix + key
	now := time.Now().UnixNano()
	var until int64
	err = tx.QueryRowContext(ctx, `SELECT until FROM trend_lease WHERE key = ?`, key).Scan(&until)
	switch {
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		return nil, false, err
	case until > now:
		return func(context.Context) error { return nil }, false, tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO trend_lease(key, until) VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET until = excluded.until
	`, key, now+int64(ttl)); err != nil {
		return nil, false, err
	}
	if err = tx.Commit(); err != nil {
		return nil, false, err
	}
	return func(ctx context.Context) error {
		_, err := s.db.ExecContext(ctx, `DELETE FROM trend_lease WHERE key = ?`, key)
		return err
	}, true, nil
}

func (s *store) Close() error {
	if !s.own {
		return nil
	}
	return s.db.Close()
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS trend (
			key TEXT PRIMARY KEY,
			value BLOB NOT NULL
		);
		CREATE TABLE IF NOT EXISTS trend_lease (
			key TEXT PRIMARY KEY,
			until INTEGER NOT NULL
		);
	`)
	return err
}

func sqlitePath(u *url.URL) string {
	if u.Opaque != "" {
		return u.Opaque
	}
	path := u.Host + u.Path
	switch path {
	case "", ":memory:", "/:memory:":
		return ":memory:"
	}
	return filepath.Clean(path)
}
