package postgresql

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/viper"
)

var Pool *pgxpool.Pool

func InitDB() error {
	ctx := context.Background()
	writeDSN := viper.GetString("postgresql.writeDSN")
	config, err := pgxpool.ParseConfig(writeDSN)
	if err != nil {
		log.Fatalf("parse config failed: %v", err)
		return err
	}

	config.MaxConns = 8
	config.MinConns = 4
	config.MaxConnIdleTime = 10 * time.Minute
	config.MaxConnLifetime = 1 * time.Hour
	config.HealthCheckPeriod = 30 * time.Second

	Pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
		return err
	}

	log.Println("postgresql 连接池初始化成功")
	return nil
}

type Record struct {
	Content   string    `json:"content"`
	Text      string    `json:"text"`
	OriginId  string    `json:"id"`
	Title     string    `json:"title"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	AccountId int       `json:"account_id"`
}

func BatchInsert(rows []Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if len(rows) == 0 {
		return nil
	}

	// 构建批量插入 SQL
	baseQuery := `INSERT INTO article (created_at, updated_at, title, content, account_id, origin_id) VALUES `
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*6)

	for i, item := range rows {
		// 每 6 个参数一组
		start := i*6 + 1
		valueStrings = append(valueStrings,
			fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				start, start+1, start+2, start+3, start+4, start+5,
			),
		)
		valueArgs = append(valueArgs,
			item.CreatedAt, item.UpdatedAt, item.Title, item.Content, item.AccountId, item.OriginId,
		)
	}

	query := baseQuery + strings.Join(valueStrings, ",")
	_, err := Pool.Exec(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("batch insert error: %w", err)
	}

	fmt.Printf("批量插入成功: %d 条记录\n", len(rows))
	return nil
}
