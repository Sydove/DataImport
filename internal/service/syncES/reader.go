package syncES

import (
	"DataImport/internal/db/postgresql"
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type PGReader struct {
	pageSize int
	timeout  time.Duration
}

func NewPGReader(pageSize int, timeout time.Duration) *PGReader {
	return &PGReader{
		pageSize: pageSize,
		timeout:  timeout,
	}
}

// ReadBatch
//
//	@Description: 从数据库读取文章记录批次
//	@receiver r
//	@param ctx
//	@param startID
//	@return []ArticleRecord
//	@return error
func (r *PGReader) ReadBatch(ctx context.Context, startID, snapshotMaxID int) ([]ArticleRecord, error) {
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	rows, err := postgresql.Pool.Query(
		queryCtx,
		"SELECT content, origin_id, id, title, created_at, account_id FROM article WHERE id > $1 AND id <= $2 ORDER BY id LIMIT $3",
		startID,
		snapshotMaxID,
		r.pageSize,
	)
	if err != nil {
		return nil, fmt.Errorf("query postgres batch: %w", err)
	}
	defer rows.Close()

	records, err := pgx.CollectRows(rows, pgx.RowToStructByName[ArticleRecord])
	if err != nil {
		return nil, fmt.Errorf("collect postgres rows: %w", err)
	}

	for i := range records {
		records[i].CreatedAt = records[i].CreatedAt.Truncate(time.Millisecond)
	}
	return records, nil
}
