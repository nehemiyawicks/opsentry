package storage

import (
	"context"
	"database/sql"
	"encoding/json"
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
		`CREATE TABLE IF NOT EXISTS alerts (
			fingerprint TEXT PRIMARY KEY,
			monitor_id  TEXT NOT NULL,
			chain       TEXT NOT NULL,
			block_num   INTEGER NOT NULL,
			block_hash  BLOB NOT NULL,
			kind        TEXT NOT NULL,
			severity    TEXT,
			receivers   TEXT NOT NULL DEFAULT '[]',
			at          INTEGER NOT NULL,
			payload     TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alerts_chain_block ON alerts(chain, block_num)`,
		`CREATE INDEX IF NOT EXISTS alerts_monitor ON alerts(monitor_id)`,
		`CREATE TABLE IF NOT EXISTS abi_cache (
			chain_id   INTEGER NOT NULL,
			address    BLOB NOT NULL,
			abi_json   TEXT NOT NULL,
			fetched_at INTEGER NOT NULL,
			PRIMARY KEY (chain_id, address)
		)`,
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

func (s *SQLiteStore) IsDuplicate(ctx context.Context, fingerprint string) (bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT 1 FROM alerts WHERE fingerprint = ? LIMIT 1`, fingerprint)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *SQLiteStore) RecordAlert(ctx context.Context, a pipeline.Alert) error {
	payload, err := json.Marshal(pipeline.AlertEnv(a))
	if err != nil {
		return fmt.Errorf("marshal alert env: %w", err)
	}
	receivers, err := json.Marshal(a.Match.Receivers)
	if err != nil {
		return fmt.Errorf("marshal receivers: %w", err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO alerts (fingerprint, monitor_id, chain, block_num, block_hash, kind, severity, receivers, at, payload)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(fingerprint) DO NOTHING`,
		a.Fingerprint,
		a.Match.Event.MonitorID,
		a.Match.Event.Log.Chain,
		int64(a.Match.Event.Log.Block.Number),
		a.Match.Event.Log.Block.Hash[:],
		string(a.Kind),
		a.Match.Severity,
		string(receivers),
		a.At.Unix(),
		string(payload),
	)
	return err
}

func (s *SQLiteStore) LoadAlertsAtBlock(ctx context.Context, chain string, blockNumber uint64, blockHash [32]byte) ([]StoredAlert, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT fingerprint, monitor_id, severity, receivers, payload
		 FROM alerts
		 WHERE chain = ? AND block_num = ? AND block_hash = ? AND kind = 'firing'`,
		chain, int64(blockNumber), blockHash[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredAlert
	for rows.Next() {
		var sa StoredAlert
		var receivers, payload string
		if err := rows.Scan(&sa.Fingerprint, &sa.MonitorID, &sa.Severity, &receivers, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(receivers), &sa.Receivers); err != nil {
			return nil, fmt.Errorf("decode receivers: %w", err)
		}
		if err := json.Unmarshal([]byte(payload), &sa.Env); err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
		out = append(out, sa)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) LoadCachedABI(ctx context.Context, chainID uint64, address [20]byte) ([]byte, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT abi_json FROM abi_cache WHERE chain_id = ? AND address = ?`,
		int64(chainID), address[:])
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return []byte(raw), true, nil
}

func (s *SQLiteStore) SaveCachedABI(ctx context.Context, chainID uint64, address [20]byte, abiJSON []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO abi_cache (chain_id, address, abi_json, fetched_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(chain_id, address) DO UPDATE SET
			abi_json   = excluded.abi_json,
			fetched_at = excluded.fetched_at`,
		int64(chainID), address[:], string(abiJSON), time.Now().Unix())
	return err
}

func arr32(b []byte) [32]byte {
	var a [32]byte
	copy(a[:], b)
	return a
}
