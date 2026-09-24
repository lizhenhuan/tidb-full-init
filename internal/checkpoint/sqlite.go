// SQLite 权威位点存储（M2 T2.3，设计文档 D6/D10）。
// 纯 Go 驱动 modernc.org/sqlite：无 CGO，保持单二进制部署（与 D1 同一约束）。
//
// 位点语义（M1.5 P1#9 定稿）：offset = "已应用的最后一个 offset"（last applied）；
// 无记录返回 -1，从分区头开始消费。
package checkpoint

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"

	"github.com/lizhenhuan/tidb-full-init/internal/model"
)

// SQLiteStore 基于 SQLite 的 OffsetStore。
type SQLiteStore struct {
	db *sql.DB
}

// NewSQLiteStore 打开（或创建）位点库并确保表存在。
// Save 必须在目标端事务提交成功之后调用（flush-后-commit，D2/D10）。
func NewSQLiteStore(ctx context.Context, path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("checkpoint open %s: %w", path, err)
	}
	const ddl = `CREATE TABLE IF NOT EXISTS offset_checkpoint (
	  topic        TEXT NOT NULL,
	  partition_id INTEGER NOT NULL,
	  offset_id    INTEGER NOT NULL,
	  updated_at   INTEGER NOT NULL DEFAULT (unixepoch()),
	  PRIMARY KEY (topic, partition_id)
	)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		db.Close()
		return nil, fmt.Errorf("checkpoint create table: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

// Load 返回分区已应用的最后一个 offset；无记录返回 -1。
func (s *SQLiteStore) Load(ctx context.Context, tp model.TopicPartition) (int64, error) {
	var off int64
	err := s.db.QueryRowContext(ctx,
		`SELECT offset_id FROM offset_checkpoint WHERE topic = ? AND partition_id = ?`,
		tp.Topic, tp.Partition).Scan(&off)
	if err == sql.ErrNoRows {
		return -1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("checkpoint load %s/%d: %w", tp.Topic, tp.Partition, err)
	}
	return off, nil
}

// Save 持久化"已应用的最后一个 offset"（单调前进，写小值会被忽略，
// 防止乱序回退位点导致重复回放）。
func (s *SQLiteStore) Save(ctx context.Context, tp model.TopicPartition, offset int64) error {
	res, err := s.db.ExecContext(ctx, `
	  INSERT INTO offset_checkpoint (topic, partition_id, offset_id) VALUES (?, ?, ?)
	  ON CONFLICT(topic, partition_id) DO UPDATE SET
	    offset_id = excluded.offset_id, updated_at = unixepoch()
	  WHERE excluded.offset_id > offset_checkpoint.offset_id`,
		tp.Topic, tp.Partition, offset)
	if err != nil {
		return fmt.Errorf("checkpoint save %s/%d@%d: %w", tp.Topic, tp.Partition, offset, err)
	}
	// 冲突但未更新 = 旧值已更大，视为幂等成功（RowsAffected=0 不报错）
	_, _ = res.RowsAffected()
	return nil
}

// All 返回全部分区当前位点（观测/展示用）。
func (s *SQLiteStore) All(ctx context.Context) (map[model.TopicPartition]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT topic, partition_id, offset_id FROM offset_checkpoint`)
	if err != nil {
		return nil, fmt.Errorf("checkpoint all: %w", err)
	}
	defer rows.Close()
	out := make(map[model.TopicPartition]int64)
	for rows.Next() {
		var tp model.TopicPartition
		var off int64
		if err := rows.Scan(&tp.Topic, &tp.Partition, &off); err != nil {
			return nil, err
		}
		out[tp] = off
	}
	return out, rows.Err()
}

// Close 关闭底层连接。
func (s *SQLiteStore) Close() error { return s.db.Close() }

var _ OffsetStore = (*SQLiteStore)(nil)
