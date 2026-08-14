package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &SQLiteStore{db: db}, nil
}

func migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS cursors (
			chain TEXT PRIMARY KEY,
			block_number INTEGER NOT NULL,
			block_hash BLOB NOT NULL,
			parent_hash BLOB NOT NULL,
			block_time INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS blocks (
			chain TEXT NOT NULL,
			block_number INTEGER NOT NULL,
			block_hash BLOB NOT NULL,
			parent_hash BLOB NOT NULL,
			block_time INTEGER NOT NULL,
			PRIMARY KEY (chain, block_number)
		)`,
		`CREATE INDEX IF NOT EXISTS blocks_chain_num_desc ON blocks(chain, block_number DESC)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func (s *SQLiteStore) LoadCursor(ctx context.Context, chain string) (Cursor, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT block_number, block_hash, parent_hash, block_time FROM cursors WHERE chain = ?`, chain)
	var num int64
	var hash, parent []byte
	var t int64
	if err := row.Scan(&num, &hash, &parent, &t); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Cursor{}, false, nil
		}
		return Cursor{}, false, err
	}
	return Cursor{
		Chain: chain,
		Block: pipeline.BlockRef{
			Chain:      chain,
			Number:     uint64(num),
			Hash:       arr32(hash),
			ParentHash: arr32(parent),
			Time:       time.Unix(t, 0).UTC(),
		},
	}, true, nil
}

func (s *SQLiteStore) SaveCursor(ctx context.Context, c Cursor) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cursors (chain, block_number, block_hash, parent_hash, block_time, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chain) DO UPDATE SET
			block_number = excluded.block_number,
			block_hash   = excluded.block_hash,
			parent_hash  = excluded.parent_hash,
			block_time   = excluded.block_time,
			updated_at   = excluded.updated_at`,
		c.Chain, int64(c.Block.Number), c.Block.Hash[:], c.Block.ParentHash[:], c.Block.Time.Unix(), time.Now().Unix())
	return err
}

func (s *SQLiteStore) RememberBlock(ctx context.Context, b pipeline.BlockRef) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO blocks (chain, block_number, block_hash, parent_hash, block_time)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(chain, block_number) DO UPDATE SET
			block_hash  = excluded.block_hash,
			parent_hash = excluded.parent_hash,
			block_time  = excluded.block_time`,
		b.Chain, int64(b.Number), b.Hash[:], b.ParentHash[:], b.Time.Unix())
	return err
}

func (s *SQLiteStore) LoadRecentBlocks(ctx context.Context, chain string, minBlockNumber uint64) ([]pipeline.BlockRef, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT block_number, block_hash, parent_hash, block_time FROM blocks
		 WHERE chain = ? AND block_number >= ? ORDER BY block_number`,
		chain, int64(minBlockNumber))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pipeline.BlockRef
	for rows.Next() {
		var num int64
		var hash, parent []byte
		var t int64
		if err := rows.Scan(&num, &hash, &parent, &t); err != nil {
			return nil, err
		}
		out = append(out, pipeline.BlockRef{
			Chain:      chain,
			Number:     uint64(num),
			Hash:       arr32(hash),
			ParentHash: arr32(parent),
			Time:       time.Unix(t, 0).UTC(),
		})
	}
	return out, rows.Err()
}

func arr32(b []byte) [32]byte {
	var a [32]byte
	copy(a[:], b)
	return a
}
