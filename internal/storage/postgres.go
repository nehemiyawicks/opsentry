package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nehemiyawicks/opsentry/internal/pipeline"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func OpenPostgres(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx new: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgx ping: %w", err)
	}
	s := &PostgresStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS cursors (
			chain        TEXT PRIMARY KEY,
			block_number BIGINT NOT NULL,
			block_hash   BYTEA NOT NULL,
			parent_hash  BYTEA NOT NULL,
			block_time   BIGINT NOT NULL,
			updated_at   BIGINT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS blocks (
			chain        TEXT NOT NULL,
			block_number BIGINT NOT NULL,
			block_hash   BYTEA NOT NULL,
			parent_hash  BYTEA NOT NULL,
			block_time   BIGINT NOT NULL,
			PRIMARY KEY (chain, block_number)
		)`,
		`CREATE INDEX IF NOT EXISTS blocks_chain_num_desc ON blocks(chain, block_number DESC)`,
		`CREATE TABLE IF NOT EXISTS alerts (
			fingerprint TEXT PRIMARY KEY,
			monitor_id  TEXT NOT NULL,
			chain       TEXT NOT NULL,
			block_num   BIGINT NOT NULL,
			block_hash  BYTEA NOT NULL,
			kind        TEXT NOT NULL,
			severity    TEXT,
			receivers   TEXT NOT NULL DEFAULT '[]',
			at          BIGINT NOT NULL,
			payload     TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS alerts_chain_block ON alerts(chain, block_num)`,
		`CREATE INDEX IF NOT EXISTS alerts_monitor ON alerts(monitor_id)`,
		`CREATE TABLE IF NOT EXISTS abi_cache (
			chain_id   BIGINT NOT NULL,
			address    BYTEA NOT NULL,
			abi_json   TEXT NOT NULL,
			fetched_at BIGINT NOT NULL,
			PRIMARY KEY (chain_id, address)
		)`,
	}
	for _, sql := range stmts {
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) LoadCursor(ctx context.Context, chain string) (Cursor, bool, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT block_number, block_hash, parent_hash, block_time FROM cursors WHERE chain = $1`, chain)
	var num int64
	var hash, parent []byte
	var t int64
	if err := row.Scan(&num, &hash, &parent, &t); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

func (s *PostgresStore) SaveCursor(ctx context.Context, c Cursor) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO cursors (chain, block_number, block_hash, parent_hash, block_time, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (chain) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash   = EXCLUDED.block_hash,
			parent_hash  = EXCLUDED.parent_hash,
			block_time   = EXCLUDED.block_time,
			updated_at   = EXCLUDED.updated_at`,
		c.Chain, int64(c.Block.Number), c.Block.Hash[:], c.Block.ParentHash[:], c.Block.Time.Unix(), time.Now().Unix())
	return err
}

func (s *PostgresStore) RememberBlock(ctx context.Context, b pipeline.BlockRef) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO blocks (chain, block_number, block_hash, parent_hash, block_time)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (chain, block_number) DO UPDATE SET
			block_hash  = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			block_time  = EXCLUDED.block_time`,
		b.Chain, int64(b.Number), b.Hash[:], b.ParentHash[:], b.Time.Unix())
	return err
}

func (s *PostgresStore) LoadRecentBlocks(ctx context.Context, chain string, minBlockNumber uint64) ([]pipeline.BlockRef, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT block_number, block_hash, parent_hash, block_time FROM blocks
		 WHERE chain = $1 AND block_number >= $2 ORDER BY block_number`,
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

func (s *PostgresStore) IsDuplicate(ctx context.Context, fingerprint string) (bool, error) {
	row := s.pool.QueryRow(ctx, `SELECT 1 FROM alerts WHERE fingerprint = $1 LIMIT 1`, fingerprint)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) RecordAlert(ctx context.Context, a pipeline.Alert) error {
	payload, err := json.Marshal(pipeline.AlertEnv(a))
	if err != nil {
		return fmt.Errorf("marshal alert env: %w", err)
	}
	receivers, err := json.Marshal(a.Match.Receivers)
	if err != nil {
		return fmt.Errorf("marshal receivers: %w", err)
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO alerts (fingerprint, monitor_id, chain, block_num, block_hash, kind, severity, receivers, at, payload)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (fingerprint) DO NOTHING`,
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

func (s *PostgresStore) LoadAlertsAtBlock(ctx context.Context, chain string, blockNumber uint64, blockHash [32]byte) ([]StoredAlert, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT fingerprint, monitor_id, severity, receivers, payload
		 FROM alerts
		 WHERE chain = $1 AND block_num = $2 AND block_hash = $3 AND kind = 'firing'`,
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

func (s *PostgresStore) LoadCachedABI(ctx context.Context, chainID uint64, address [20]byte) ([]byte, bool, error) {
	row := s.pool.QueryRow(ctx,
		`SELECT abi_json FROM abi_cache WHERE chain_id = $1 AND address = $2`,
		int64(chainID), address[:])
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return []byte(raw), true, nil
}

func (s *PostgresStore) SaveCachedABI(ctx context.Context, chainID uint64, address [20]byte, abiJSON []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO abi_cache (chain_id, address, abi_json, fetched_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET
			abi_json   = EXCLUDED.abi_json,
			fetched_at = EXCLUDED.fetched_at`,
		int64(chainID), address[:], string(abiJSON), time.Now().Unix())
	return err
}

func (s *PostgresStore) ClearABICache(ctx context.Context) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM abi_cache`)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}
